"""Workflow subpackage for ScaleMS execution dispatching to RADICAL Pilot.

Manage workflow context for RADICAL Pilot.

Command Line Invocation Example:
    ``python -m scalems.radical --resource local.localhost --venv $HOME/myvenv --access
    local myworkflow.py``

For required and optional command line arguments:
    ``python -m scalems.radical --help``

The user is largely responsible for establishing appropriate
`RADICAL Cybertools <https://radical-cybertools.github.io/>`__
(RCT) software environment at both the client side
and the execution side.

For performance and control, the canonical use case is a fully static `venv`
configuration for both the Pilot agent (and bootstrapping) interpreter, remote RCT
stack, and executed Tasks. However, the default behavior should work for most users,
in which a `venv` is created in the radical sandbox on the first connection (reused if it
already exists) and the RCT stack is updated within the Pilot sandbox for each session.

In the case of non-RCT Python dependencies,
:py:class:`~radical.pilot.Pilot` has an (evolving)
:py:attr:`~radical.pilot.Pilot.prepare_env`
feature that can be used for a Task dependency
(:py:data:`~radical.pilot.TaskDescription.named_env`) to provide a dynamically
created venv with a list of requested packages.

Locally prepared package distribution archives can be used, such as by staging with the
*Pilot.stage_in* before doing *prepare_env*.

.. seealso::

    Refer to https://github.com/SCALE-MS/scale-ms/issues/141
    for the status of `scalems` support for automatic execution environment bootstrapping.

Use ``virtenv_mode=use``, ``virtenv=/path/to/venv``, ``rp_version=installed`` in the RP
resource
definition, and activate alternative Task venvs using ``pre_exec``. The user (or
client) is
then responsible for maintaining venv(s) with the correct RCT stack (matching the API
used by the client-side RCT stack), the `scalems` package, and any dependencies of the
workflow.

Upcoming RP features:

    There is some work underway to place full venvs at run time, specifically to handle
    use
    cases in which it is important to run the Python stack from a local filesystem on an
    HPC compute node. So far, this is limited to use of conda freeze.

    Upcoming RP features will provide a mechanism for environment caching so that module
    load, source ``$VENV/bin/activate``, etc. do not need to be repeated for every task.
    However, the current mechanisms for optimal (static) venv usage are

See Also:
    * `scalems.dispatching`
    * `scalems.execution`
    * `scalems.radical.runtime`
    * https://github.com/SCALE-MS/scale-ms/issues/90

"""
# TODO: Consider converting to a namespace package to improve modularity of
#  implementation.

import argparse
import asyncio
import contextlib
import dataclasses
import functools
import json
import logging
import os
import pathlib
import threading
import typing
import weakref

from radical import pilot as rp

import scalems.execution
import scalems.subprocess
import scalems.workflow
from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from scalems.exceptions import ScaleMSError
from scalems.execution import AbstractWorkflowUpdater
from scalems.execution import RuntimeManager
from .runtime import _configuration
from .runtime import _connect_rp
from .runtime import _set_configuration
from .runtime import Configuration
from .runtime import get_pre_exec
from .runtime import parser as _runtime_parser
from .runtime import Runtime
from ..identifiers import TypeIdentifier
from scalems.utility import make_parser as _make_parser

logger = logging.getLogger(__name__)
logger.debug('Importing {}'.format(__name__))

try:
    cache = functools.cache
except AttributeError:
    # Note: functools.cache does not appear until Python 3.9
    cache = functools.lru_cache(maxsize=None)

parser = _make_parser(__package__, parents=[_runtime_parser()])


def configuration(*args, **kwargs) -> Configuration:
    """Get (and optionally set) the RADICAL Pilot configuration.

    With no arguments, returns the current configuration. If a configuration has
    not yet been set, the command line parser is invoked to try to build a new
    configuration.

    If arguments are provided, try to construct a scalems.radical.Configuration
    and use it to initialize the module.

    It is an error to try to initialize the module more than once.
    """
    # Not thread-safe
    if len(args) > 0:
        _set_configuration(*args, **kwargs)
    elif len(kwargs) > 0:
        _set_configuration(
            Configuration(**kwargs)
        )
    elif _configuration.get(None) is None:
        # No config is set yet. Generate with module parser.
        c = Configuration()
        parser.parse_known_args(namespace=typing.cast(argparse.Namespace, c))
        _configuration.set(c)
    return _configuration.get()


def executor_factory(manager: scalems.workflow.WorkflowManager,
                     params: Configuration = None):
    if params is not None:
        _set_configuration(params)
    params = configuration()

    executor = RPDispatchingExecutor(source=manager,
                                     loop=manager.loop(),
                                     configuration=params,
                                     dispatcher_lock=manager._dispatcher_lock)
    return executor


def workflow_manager(loop: asyncio.AbstractEventLoop, directory=None):
    """Manage a workflow context for RADICAL Pilot work loads.

    The rp.Session is created when the Python Context Manager is "entered",
    so the asyncio event loop must be running before then.

    To help enforce this, we use an async Context Manager, at least in the
    initial implementation. However, the implementation is not thread-safe.
    It is not reentrant, but this is not checked. We probably _do_ want to
    constrain ourselves to zero or one Sessions per environment, but we _do_
    need to support multiple Pilots and task submission scopes (_resource
    requirement groups).
    Further discussion is welcome.

    Warning:
        The importer of this module should be sure to import radical.pilot
        before importing the built-in logging module to avoid spurious warnings.
    """
    return scalems.workflow.WorkflowManager(
        loop=loop,
        executor_factory=executor_factory,
        directory=directory
    )


class RPResult:
    """Basic result type for RADICAL Pilot tasks.

    Define a return type for Futures or awaitable tasks from
    RADICAL Pilot commands.
    """
    # TODO: Provide support for RP-specific versions of standard SCALEMS result types.


class RPTaskFailure(ScaleMSError):
    """Error in radical.pilot.Task execution.

    Attributes:
        failed_task: A dictionary representation of the failed task.

    TODO: What can/should we capture and report from the failed task?
    """
    failed_task: dict

    def __init__(self, *args, task: rp.Task):
        super().__init__(*args)
        self.failed_task = task.as_dict()


class RPInternalError(ScaleMSError):
    """RADICAL Pilot is misbehaving, probably due to a bug.

    Please report the potential bug.
    """


class RPFinalTaskState:
    # TODO: Provide a bridge between the threading and asyncio Event primitives so that
    #  we can effectively `await` for the events. Presumably this would look like an
    #  asyncio.Future created from a threading.Future of a watcher of the three Events.
    #  Use asyncio.wrap_future() to wrap a threading.Future to an asyncio.Future.
    #  If there is not an adapter, we can loop.run_in_executor() to run the
    #  threading.Event watcher in a separate thread.
    # For future reference, check out the `sched` module.
    def __init__(self):
        self.canceled = threading.Event()
        self.done = threading.Event()
        self.failed = threading.Event()

    def __bool__(self):
        return self.canceled.is_set() or self.done.is_set() or self.failed.is_set()


def _rp_callback(obj: rp.Task,
                 state,
                 cb_data: weakref.ReferenceType = None,
                 final: RPFinalTaskState = None):
    """Prototype for RP.Task callback.

    To use, partially bind the *final* parameter (with functools.partial) to get a
    callable with the RP.Task callback signature.

    Register with *task* to be called when the rp.Task state changes.
    """
    if final is None:
        raise APIError(
            'This function is strictly for dynamically prepared RP callbacks through '
            'functools.partial.')
    logger.debug(f'Callback triggered by {repr(obj)} state change to {repr(state)}.')
    try:
        # Note: assertions and exceptions are not useful in RP callbacks.
        if state in (rp.states.DONE, rp.states.CANCELED, rp.states.FAILED):
            # TODO: Pending https://github.com/radical-cybertools/radical.pilot/issues/2444
            # tmgr: rp.TaskManager = obj.tmgr
            # ref = cb_data()
            # tmgr.unregister_callback(cb=ref, metrics='TASK_STATE',
            #                          uid=obj.uid)
            logger.debug(f'Recording final state {state} for {repr(obj)}')
            if state == rp.states.DONE:
                final.done.set()
            elif state == rp.states.CANCELED:
                final.canceled.set()
            elif state == rp.states.FAILED:
                final.failed.set()
            else:
                logger.error('Bug: logic error in state cases.')
    except Exception as e:
        logger.error(f'Exception encountered during rp.Task callback: {repr(e)}')


async def _rp_task_watcher(task: rp.Task,  # noqa: C901
                           final: RPFinalTaskState,
                           ready: asyncio.Event) -> rp.Task:
    """Manage the relationship between an RP.Task and a scalems Future.

    Cancel the RP.Task if this task or the scalems.Future is canceled.

    Publish the RP.Task result or cancel the scalems.Future if the RP.Task is
    done or canceled.

    Arguments:
        task: RADICAL Pilot Task, submitted by caller.
        final: thread-safe event handler for the RP task call-back to announce it has run.
        ready: output parameter, set when coroutine has run enough to perform its
        responsibilities.

    Returns:
        *task* in its final state.

    An asyncio.Future based on this coroutine has very similar semantics to the
    required *future* argument, but this is subject to change.
    The coroutine is intended to facilitate progress of the task,
    regardless of the rp.Task results. The provided *future* allows the rp.Task
    results to be interpreted and semantically translated. rp.Task failure is
    translated into an exception on *future*. The *future* has a different
    exposure than the coroutine return value, as well: again, the *future* is
    connected to the workflow item and user-facing interface, whereas this
    coroutine is a detail of the task management. Still, these two modes of output
    are subject to revision without notice.

    Caller should await the *ready* event before assuming the watcher task is doing its
    job.
    """

    async def wait_for_final(state: RPFinalTaskState) -> RPFinalTaskState:
        """Function to watch for final event.

        This is a poor design. We should replace with a queuing system for state
        transitions as described in scalems.workflow.
        """
        while not state:
            # TODO: What is the right adapter between asyncio and threading event waits?
            await asyncio.sleep(0.05)
        return state

    event_watcher = asyncio.create_task(wait_for_final(final))

    try:
        ready.set()

        while not event_watcher.done():
            # TODO(#96): Use a control thread to manage *threading* primitives and
            #  translate to asyncio primitives.
            # Let the watcher wake up periodically to check for suspicious state.

            _rp_task_was_complete = task.state in rp.FINAL

            done, pending = await asyncio.wait([event_watcher],
                                               timeout=60.0,
                                               return_when=asyncio.FIRST_COMPLETED)

            if _rp_task_was_complete and not event_watcher.done():
                event_watcher.cancel()
                raise RPInternalError('RP Callbacks are taking too long to complete. '
                                      f'Abandoning {repr(task)}. Please report bug.')

            if event_watcher in done:
                assert final
                logger.debug(f'Handling finalization for RP task {task.uid}.')
                if final.failed.is_set():
                    # TODO(#92): Provide more useful error feedback.
                    raise RPTaskFailure(f'{task.uid} failed.', task=task)
                elif final.canceled.is_set():
                    # Act as if RP called Task.cancel() on us.
                    raise asyncio.CancelledError()
                assert final.done.is_set()

                logger.debug(
                    f'Publishing results from RP Task {task.uid}.')
                # TODO: Manage result type.
                return task

        raise scalems.exceptions.InternalError(
            'Logic error. This line should not have been reached.')

    except asyncio.CancelledError as e:
        logger.debug(
            f'Received cancellation in watcher task for {repr(task)}')
        if task.state not in rp.CANCELED:
            logger.debug(f'Propagating cancellation to {repr(task)}.')
            task.cancel()
        raise e


async def rp_task(rptask: rp.Task) -> asyncio.Task:
    """Mediate between a radical.pilot.Task and an asyncio.Future.

    Schedule an asyncio Task to receive the result of the RP Task. The asyncio
    Task must also make sure that asyncio cancellation propagates to the rp.Task.cancel,
    and vice versa.

    This function should be awaited immediately to make sure the necessary call-backs
    get registered. The result will be an asyncio.Task, which should be awaited
    separately.

    Internally, this function provides a call-back to the rp.Task. The call-back
    provided to RP cannot directly call asyncio.Future methods (such as set_result() or
    set_exception()) because RP will be making the call from another thread without
    mediation by the asyncio event loop.

    As such, we provide a thread-safe event handler to propagate the
    RP Task call-back to to this asyncio.Task result.
    (See `_rp_callback()` and `RPFinalTaskState`)

    Canceling the returned task will cause *rptask* to be canceled.
    Canceling *rptask* will cause this task to be canceled.

    Arguments:
        rptask: RADICAL Pilot Task that has already been submitted.

    Returns:
        A Task that, when awaited, returns the rp.Task instance in its final state.
    """
    if not isinstance(rptask, rp.Task):
        raise TypeError('Function requires a RADICAL Pilot Task object.')

    final = RPFinalTaskState()
    callback = functools.partial(_rp_callback, final=final)
    functools.update_wrapper(callback, _rp_callback)

    # Note: register_callback() does not provide a return value to use for
    # TaskManager.unregister_callback and we cannot provide *callback* with a reference
    # to itself until after it is created, so we will get a reference here that we can
    # provide through the *cb_data* argument of rp.Task callbacks.
    cb_data: weakref.ReferenceType = weakref.ref(callback)

    rptask.register_callback(callback, cb_data=cb_data, metric=rp.constants.TASK_STATE)

    if rptask.state in rp.FINAL:
        # rptask may have reached FINAL state before callback was registered.
        # Call it once. For simplicity, let the task_watcher logic proceed normally.
        logger.warning(f'RP Task {repr(rptask)} finished suspiciously fast.')
        callback(rptask, rptask.state, cb_data)

    asyncio.get_running_loop().slow_callback_duration = 0.2
    watcher_started = asyncio.Event()
    waiter = asyncio.create_task(watcher_started.wait())
    wrapped_task = asyncio.create_task(_rp_task_watcher(task=rptask,
                                                        final=final,
                                                        ready=watcher_started))

    # Make sure that the task is cancellable before returning it to the caller.
    await asyncio.wait((waiter, wrapped_task),
                       return_when=asyncio.FIRST_COMPLETED)
    if wrapped_task.done():
        # Let CancelledError propagate.
        e = wrapped_task.exception()
        if e is not None:
            raise e
    # watcher_task.
    return wrapped_task


def _describe_legacy_task(item: scalems.workflow.Task,
                          pre_exec: list) -> rp.TaskDescription:
    """Derive a RADICAL Pilot TaskDescription from a scalems workflow item.

    For a "raptor" style task, see _describe_raptor_task()
    """
    subprocess_type = TypeIdentifier(('scalems', 'subprocess', 'SubprocessTask'))
    assert item.description().type() == subprocess_type
    input_data = item.input
    task_input = scalems.subprocess.SubprocessInput(**input_data)
    args = list([arg for arg in task_input.argv])
    # Warning: TaskDescription class does not have a strongly defined interface.
    # Check docs for schema.
    task_description = rp.TaskDescription(from_dict=dict(executable=args[0],
                                                         arguments=args[1:],
                                                         stdout=str(task_input.stdout),
                                                         stderr=str(task_input.stderr),
                                                         pre_exec=pre_exec))
    uid: str = item.uid().hex()
    task_description.uid = uid

    # TODO: Check for and activate an appropriate venv
    # using
    #     task_description.pre_exec = ...
    # or
    #     task_description.named_env = ...

    # TODO: Interpret item details and derive appropriate staging directives.
    task_description.input_staging = list(task_input.inputs.values())
    task_description.output_staging = [{
        'source': str(task_input.stdout),
        'target': os.path.join(uid, pathlib.Path(task_input.stdout).name),
        'action': rp.TRANSFER
    }, {
        'source': str(task_input.stderr),
        'target': os.path.join(uid, pathlib.Path(task_input.stderr).name),
        'action': rp.TRANSFER
    }]
    task_description.output_staging += task_input.outputs.values()

    return task_description


def _describe_raptor_task(item: scalems.workflow.Task,
                          scheduler: str,
                          pre_exec: list) -> rp.TaskDescription:
    """Derive a RADICAL Pilot TaskDescription from a scalems workflow item.

    The TaskDescription will be submitted to the named *scheduler*,
    where *scheduler* is the UID of a task managing the life of a rp.raptor.Master
    instance.

    Caller is responsible for ensuring that *scheduler* is valid.
    """
    # Warning: TaskDescription class does not have a strongly defined interface.
    # Check docs for schema.
    # Ref: scalems_rp_master._RaptorTaskDescription
    task_description = rp.TaskDescription(
        from_dict=dict(
            executable='scalems',  # This value is currently ignored, but must be set.
            pre_exec=pre_exec
        )
    )
    task_description.uid = item.uid()
    task_description.scheduler = str(scheduler)
    # Example work would be the JSON serialized form of the following dictionary.
    # {'mode': 'call',
    #  'cores': 1,
    #  'timeout': 10,
    #  'data': {'method': 'hello',
    #           'kwargs': {'world': uid}}}
    #
    # Maybe something like this:
    # work_dict = {
    #     'mode': 'scalems',
    #     'cores': 1,
    #     'timeout': 10,
    #     'data': item.serialize()
    # }
    work_dict = {
        'mode': 'exec',
        'cores': 1,
        'timeout': None,
        'data': {
            'exe': item.input['argv'][0],
            'args': item.input['argv'][1:]
        }
    }
    task_description.arguments = [json.dumps(work_dict)]

    # TODO: Check for and activate an appropriate venv
    # using
    #     task_description.pre_exec = ...
    # or
    #     task_description.named_env = ...

    # TODO: Interpret item details and derive appropriate staging directives.
    task_description.input_staging = []
    task_description.output_staging = []

    return task_description


async def submit(*,
                 item: scalems.workflow.Task,
                 task_manager: rp.TaskManager,
                 pre_exec: list,
                 scheduler: str = None) -> asyncio.Task:
    """Dispatch a WorkflowItem to be handled by RADICAL Pilot.

    Submits an rp.Task and returns an asyncio.Task watcher for the submitted task.

    Creates a Future, registering a done_callback to publish the task result with
    *item.set_result()*.

    A callback is registered with the rp.Task to set an Event on completion. An
    asyncio.Task watcher task monitors the Event(s) and gets a reference to an
    asyncio.Future through which results can be published to the scalems workflow item.
    (Currently the Future is created in this function, but should probably be acquired
    directly from the *item* itself.) The watcher task waits for the rp.Task
    finalization event or for the Future to be cancelled. Periodically, the watcher
    task "wakes up" to check if something has gone wrong, such as the rp.Task
    completing without setting the finalization event.

    Caveats:
        There is an unavoidable race condition in the check performed by the watcher
        task. We don't know how long after an rp.Task completes before its callbacks
        will run and we can't check whether the callback has been scheduled. The
        watcher task cannot be allowed to complete successfully until we know that the
        callback has either run or will never run.

        The delay between the rp.Task state change and the callback execution should be
        less than a second. We can allow the watcher to wake up occasionally (on the
        order of minutes), so we can assume that it will never take more than a full
        iteration of the waiting loop for the callback to propagate, unless there is a
        bug in RP. For simplicity, we can just note whether `rptask.state in rp.FINAL`
        before the watcher goes to sleep and raise an error if the callback is not
        triggered in an iteration where we have set such a flag.

        If our watcher sends a cancellation to the rp.Task, there is no need to
        continue to monitor the rp.Task state and the watcher may exit.

    Args:
        item: The workflow item to be submitted
        task_manager: A radical.pilot.TaskManager instance
                      through which the task should be submitted.
        pre_exec: `radical.pilot.Task.pre_exec` prototype.
        scheduler (str): The string name of the "scheduler," corresponding to
                         the UID of a Task running a rp.raptor.Master.

    Returns:
         asyncio.Task: a "Future[rp.Task]" for a rp.Task in its final state.

    The caller *must* await the result of the coroutine to obtain an asyncio.Task that
    can be cancelled or awaited as a proxy to direct RP task management. The Task will
    hold a coroutine that is guaranteed to already be running, failed, or canceled. The
    caller should check the status of the task immediately before making assumptions
    about whether a Future has been successfully bound to the managed workflow item.

    The returned asyncio.Task can be used to cancel the rp.Task (and the Future)
    or to await the RP.Task cleanup.

    To submit tasks as a batch, await an array of submit() results in the
    same dispatching context. (TBD)
    """

    # TODO: Optimization: skip tasks that are already done (cached results available).
    def scheduler_is_ready(scheduler):
        return isinstance(scheduler, str) \
               and len(scheduler) > 0 \
               and isinstance(task_manager.get_tasks(scheduler), rp.Task)

    subprocess_type = TypeIdentifier(('scalems', 'subprocess', 'SubprocessTask'))
    if item.description().type() == subprocess_type:
        if scheduler is not None:
            raise DispatchError('Raptor not yet supported for scalems.executable.')
        rp_task_description = _describe_legacy_task(item, pre_exec=pre_exec)
    elif scheduler_is_ready(scheduler):
        # We might want a contextvars.Context to hold the current rp.Master instance name.
        rp_task_description = _describe_raptor_task(item, scheduler, pre_exec=pre_exec)
    else:
        raise APIError('Caller must provide the UID of a submitted *scheduler* task.')

    # TODO: Move slow blocking RP calls to a separate RP control thread.
    task = task_manager.submit_tasks(rp_task_description)

    rp_task_watcher = await rp_task(rptask=task)

    if rp_task_watcher.done():
        if rp_task_watcher.cancelled():
            raise DispatchError(f'Task for {item} was unexpectedly canceled during '
                                'dispatching.')
        e = rp_task_watcher.exception()
        if e is not None:
            raise DispatchError('Task for {item} failed during dispatching.') from e

    # Warning: in the long run, we should not extend the life of the reference returned
    # by edit_item, and we need to consider the robust way to publish item results.
    # TODO: Translate RP result to item result type.
    rp_task_watcher.add_done_callback(functools.partial(scalems_callback,
                                                        item=item))
    # TODO: If *item* acquires a `cancel` method, we need to subscribe to it and
    #  respond by unregistering the callback and canceling the future.

    return rp_task_watcher


def scalems_callback(fut: asyncio.Future, *, item: scalems.workflow.Task):
    """Propagate the results of a Future to the subscribed *item*.

    Partially bind *item* to use this as the argument to *fut.add_done_callback()*.

    Warning: in the long run, we should not extend the life of the reference returned
    by edit_item, and we need to consider the robust way to publish item results.

    Note:
        This is not currently RP-specific and we should look at how best to factor
        results publishing for workflow items. It may be that this function is the
        appropriate place to convert RP Task results to scalems results.
    """
    assert fut.done()
    if fut.cancelled():
        logger.info(f'Task supporting {item} has been cancelled.')
    else:
        # The following should not throw because we just checked for `done()` and
        # `cancelled()`
        e = fut.exception()
        if e:
            logger.info(f'Task supporting {item} failed: {e}')
        else:
            # TODO: Construct an appropriate scalems Result from the rp Task.
            item.set_result(fut.result())


class RPDispatchingExecutor(RuntimeManager):
    """Client side manager for work dispatched through RADICAL Pilot.

    Configuration points::
    * resource config
    * pilot config
    * session config?
    """

    def __init__(self,
                 source: scalems.workflow.WorkflowManager,
                 *,
                 loop: asyncio.AbstractEventLoop,
                 configuration: Configuration,
                 dispatcher_lock=None):
        """Create a client side execution manager.

        Initialization and de-initialization occurs through
        the Python (async) context manager protocol.
        """
        if 'RADICAL_PILOT_DBURL' not in os.environ:
            raise DispatchError('RADICAL Pilot environment is not available.')

        if not isinstance(configuration.target_venv, str) \
                or len(configuration.target_venv) == 0:
            raise ValueError(
                'Caller must specify a venv to be activated by the execution agent for '
                'dispatched tasks.')
        super().__init__(source,
                         loop=loop,
                         configuration=configuration,
                         dispatcher_lock=dispatcher_lock)

    @contextlib.contextmanager
    def runtime_configuration(self):
        """Provide scoped Configuration.

        Merge the runtime manager's configuration with the global configuration,
        update the global configuration, and yield the configuration for a ``with`` block.

        Restores the previous global configuration when exiting the ``with`` block.

        Warning:
            We do not check for re-entrance, which will cause race conditions w.r.t.
            which Context state is restored! Moreover, the Configuration object is not
            currently hashable and does not have an equality test defined.

        TODO:
            Reconsider this logic.

        Design notes:
            Do we want two-way interaction between module
            and instance configuration? Under what circumstances will one or the other
            change during execution? Should we be providing the configuration through
            the current Context, through a Context instance (usable for Context.run() or
            to the task launching command), or simply as a Configuration object?
        """

        # Get default configuration.
        configuration_dict = dataclasses.asdict(configuration())
        # Update with any internal configuration.
        if self._runtime_configuration.target_venv is not None and len(
                self._runtime_configuration.target_venv) > 0:
            configuration_dict['target_venv'] = self._runtime_configuration.target_venv
        if len(self._runtime_configuration.rp_resource_params) > 0:
            configuration_dict['rp_resource_params'].update(
                self._runtime_configuration.rp_resource_params)
        if self._runtime_configuration.execution_target is not None \
                and len(self._runtime_configuration.execution_target) > 0:
            configuration_dict[
                'execution_target'] = self._runtime_configuration.execution_target
        configuration_dict['datastore'] = self.source_context.datastore()

        c = Configuration(**configuration_dict)

        token = _configuration.set(c)
        try:
            yield c
        finally:
            _configuration.reset(token)

    async def runtime_startup(self) -> asyncio.Task:
        config: Configuration = configuration()
        self.runtime = await _connect_rp(config)

        if self.runtime is None or self.runtime.session.closed:
            raise ProtocolError('Cannot process queue without a RP Session.')

        # Launch queue processor (proxy executor).
        # TODO: Make runtime_startup optional. Let it return a resource that is
        #  provided to the normalized run_executor(), or maybe use it to configure the
        #  Submitter that will be provided to the run_executor.
        runner_started = asyncio.Event()
        runner_task = asyncio.create_task(
            scalems.execution.manage_execution(
                self,
                processing_state=runner_started))
        await runner_started.wait()
        # TODO: Note the expected scope of the runner_task lifetime with respect to
        #  the global state changes (i.e. ContextVars and locks).
        return runner_task

    def runtime_shutdown(self, runtime: Runtime):
        session = getattr(runtime, 'session', None)
        if session is None or session.closed:
            logger.error('Runtime Session is already closed?!')
        else:
            # Cancel the master.
            logger.debug('Canceling the master scheduling task.')
            task_manager = runtime.task_manager()
            if runtime.scheduler is not None:
                task_manager.cancel_tasks(uids=runtime.scheduler.uid)
                # Cancel blocks until the task is done so the following wait is
                # (currently) redundant, but there is a ticket open to change this
                # behavior.
                # See https://github.com/radical-cybertools/radical.pilot/issues/2336
                runtime.scheduler.wait(state=rp.FINAL)
                logger.debug('Master scheduling task complete.')

            # TODO: We may have multiple pilots.
            # TODO: Check for errors?
            logger.debug('Canceling Pilot.')
            runtime.pilot().cancel()
            logger.debug('Pilot canceled.')
            runtime.task_manager().close()
            logger.debug('TaskManager closed.')
            runtime.pilot_manager().close()
            logger.debug('PilotManager closed.')
            session.close()
            if session.closed:
                logger.debug('Session closed.')
            else:
                logger.error('Session not closed!')
        logger.debug('Runtime shut down.')

    def updater(self) -> 'WorkflowUpdater':
        return WorkflowUpdater(executor=self)


class WorkflowUpdater(AbstractWorkflowUpdater):
    def __init__(self, executor: RPDispatchingExecutor):
        self.executor = executor
        self.task_manager = executor.runtime.task_manager()
        # TODO: Make sure we are clear about the scope of the configuration and the
        #  life time of the workflow updater / submitter.
        self._pre_exec = list(get_pre_exec(executor.configuration()))

    async def submit(self, *, item: scalems.workflow.Task) -> asyncio.Task:
        # TODO: Ensemble handling
        item_shape = item.description().shape()
        if len(item_shape) != 1 or item_shape[0] != 1:
            raise MissingImplementationError(
                'Executor cannot handle multidimensional tasks yet.')

        task: asyncio.Task[rp.Task] = await submit(item=item,
                                                   task_manager=self.task_manager,
                                                   pre_exec=self._pre_exec)
        return task
