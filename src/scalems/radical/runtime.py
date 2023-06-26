"""Manage the RADICAL Pilot start-up and shut-down.

The provided Runtime class encapsulates stateful resources that, once acquired,
should be shut down explicitly. Runtime instances may be used as context managers to
ensure the proper protocol is followed, or the caller can take responsibility for
calling Runtime.close() to shut down.

Note: Consider whether Runtime context manager is reentrant or multi-use.

The Runtime state encapsulates several nested states. A Session, TaskManager,
and PilotManager must be created for the Runtime to be usable. Additionally, Pilots and
scheduler tasks may be added or removed during the Runtime lifetime. To better support
the alternate scenarios when a Runtime instance may be provided to a scalems.radical
component in an arbitrary state, consider making the ``with`` block scoped, such that
it only returns the Runtime instance to its initial state when exiting, rather than
shutting down completely.
See also https://docs.python.org/3/library/contextlib.html#contextlib.ExitStack

Deferred:
    Runtime can avoid providing direct access to RP interface, and instead run an
    entire RP Session state machine in a thread (separate from the asyncio event loop
    thread), relaying RP scripting commands through queues, in order to completely
    prevent misuse and to insulate the asyncio event loop from blocking RP commands.
    We need to get a better sense of the RP flow combinatorics before we can reasonably
    pursue this.

See Also:
    https://github.com/SCALE-MS/scale-ms/issues/55

.. uml::

    title scalems on radical.pilot run time

    box "SCALE-MS framework" #honeydew
    participant WorkflowManager as client_workflowmanager
    participant RuntimeManager
    participant "runner task" as scalems.execution
    end box
    box "SCALE-MS RP adapter" #linen
    participant RuntimeSession as client_runtime
    participant Executor as client_executor
    end box

    autoactivate on

    client_workflowmanager -> client_workflowmanager: async with executor
    client_workflowmanager -> client_executor: ~__aenter__()
    activate client_workflowmanager
    client_executor -> RuntimeManager: ~__aenter__() base class method
    RuntimeManager -> client_executor: runtime_configuration()
    return
    RuntimeManager -> client_executor: runtime_startup()

    client_executor -> : rp.Session()
    return
    client_executor -> client_runtime **: Session
    activate client_runtime
    client_executor -> : rp.PilotManager()
    return
    client_executor -> client_runtime: pilot_manager()
    return
    client_executor -> : rp.TaskManager()
    return
    client_executor -> client_runtime: task_manager()
    return
    client_executor -> : pilot_manager.submit_pilots()
    return
    client_executor -> client_runtime: pilot()
    note left
    Pilot venv is determined by resource definition (JSON file).
    end note
    return

    group ref [scalems.radical.raptor]
    client_executor -> client_executor: get_raptor()
    return
    end

    client_executor -> client_runtime: set raptor
    return

    client_executor ->> scalems.execution **: create_task(manage_execution)
    client_executor -> scalems.execution: await runner_started
    RuntimeManager <-- client_executor: asyncio.Task
    RuntimeManager -> RuntimeManager: set runner_task
    return
    RuntimeManager --> client_executor: self
    client_workflowmanager <-- client_executor: RuntimeManager
    client_workflowmanager --> client_workflowmanager: as manager
    deactivate RuntimeManager

    client_workflowmanager -> client_workflowmanager #gray: async with dispatcher

    ...Raptor workload handling...

    return leave dispatcher context

    == Shut down runtime ==

    client_workflowmanager -> client_executor: ~__aexit__()
    client_executor -> RuntimeManager: ~__aexit__() base class method
    RuntimeManager ->> scalems.execution: enqueue a stop control message
    deactivate scalems.execution
    RuntimeManager -> RuntimeManager: await runner_task
    note right
        drain the queue
    end note
    RuntimeManager <-- scalems.execution
    deactivate RuntimeManager
    RuntimeManager ->> client_executor: runtime_shutdown()

    client_runtime <- client_executor
    return session
    client_runtime <- client_executor
    return raptor

    group Finalize Raptor task [if raptor is not None]
        group Give Raptor some time to shut down [if raptor.state not in FINAL]
            client_executor -> : runtime.raptor.wait(timeout=60)
        end
        group Forcibly cancel Raptor task [if raptor.state not in FINAL]
            client_runtime <- client_executor
            return task_manager
            client_executor -> : task_manager.cancel_tasks()
            return
            client_runtime <- client_executor
            return raptor
            client_executor -> : runtime.raptor.wait()
            return
        end
    end

    client_executor -> : session.close()
    return

    RuntimeManager -> RuntimeManager: await runtime_shutdown()

    client_executor -> client_runtime !!
    RuntimeManager <<-- client_executor
    RuntimeManager --> RuntimeManager
    RuntimeManager --> client_executor

    client_workflowmanager <-- client_executor: leave executor context
    client_workflowmanager --> client_workflowmanager

"""

from __future__ import annotations

__all__ = (
    "configuration",
    "executor_factory",
    "parser",
    "RuntimeConfiguration",
    "RuntimeSession",
    "ScaleMSRadicalError",
)

import argparse
import asyncio
import atexit
import collections.abc
import contextlib
import contextvars
import copy
import dataclasses
import functools
import io
import json
import logging
import os
import pathlib
import shutil
import sys
import tempfile
import threading
import typing
import uuid
import warnings
import weakref
import zipfile
from typing import Awaitable

import packaging.version
import radical.saga
import radical.utils
from radical import pilot as rp

import scalems.call
import scalems.exceptions
import scalems.execution
import scalems.file
import scalems.invocation
import scalems.messages
import scalems.radical
import scalems.radical.raptor
import scalems.store
import scalems.subprocess
import scalems.workflow
from scalems.exceptions import APIError
from scalems.exceptions import DispatchError
from scalems.exceptions import InternalError
from scalems.exceptions import MissingImplementationError
from scalems.exceptions import ProtocolError
from scalems.exceptions import ScaleMSError
from .raptor import raptor_input
from ..store import FileStore
from ..execution import AbstractWorkflowUpdater
from ..identifiers import EphemeralIdentifier
from ..identifiers import TypeIdentifier

if typing.TYPE_CHECKING:
    import radical.utils as ru

logger = logging.getLogger(__name__)
logger.debug("Importing {}".format(__name__))

# TODO: Consider scoping for these context variables.
# Need to review PEP-567 and PEP-568 to consider where and how to scope the Context
# with respect to the dispatching scope.
_configuration = contextvars.ContextVar("_configuration")

# The following infrastructure logic is borrowed from concurrent.futures.thread
# What we really want, though, is to make sure that these hooks are handled before
#  the event loop is closed.
# TODO: Make sure that event loop is still active, and perform our shutdown hooks
#  before the event loop shuts down.

_shutdown = False
# Lock that ensures that new executors are not created while the interpreter is
# shutting down. Must be held while mutating _runtime_queues and _shutdown.
# Warning: don't copy this while locked, such as by forking.
_global_shutdown_lock = threading.Lock()


def _python_exit():
    global _shutdown
    with _global_shutdown_lock:
        _shutdown = True


atexit.register(_python_exit)


def _parse_option(arg: str) -> tuple:
    if not isinstance(arg, str):
        raise InternalError("Bug: This function should only be called with a str.")
    if arg.count("=") != 1:
        raise argparse.ArgumentTypeError('Expected a key/value pair delimited by "=".')
    return tuple(arg.split("="))


class ScaleMSRadicalError(ScaleMSError):
    """Base exception for `scalems.radical` backend."""


class RPConfigurationError(ScaleMSRadicalError):
    """Unusable RADICAL Pilot configuration."""


class RPConnectionError(ScaleMSRadicalError):
    """A problem connecting to the RCT back end or remote resources."""


@functools.cache
def parser(add_help=False):
    """Get the module-specific argument parser.

    Provides a base argument parser for scripts using the scalems.radical backend.

    By default, the returned ArgumentParser is created with ``add_help=False``
    to avoid conflicts when used as a *parent* for a parser more local to the caller.
    If *add_help* is provided, it is passed along to the ArgumentParser created
    in this function.

    See Also:
         https://docs.python.org/3/library/argparse.html#parents
    """
    _parser = argparse.ArgumentParser(add_help=add_help, parents=[scalems.invocation.base_parser()])

    # We could consider inferring a default venv from the VIRTUAL_ENV environment
    # variable,
    # but we currently have very poor error handling regarding venvs. For now, this needs
    # to be explicit.
    # Ref https://github.com/SCALE-MS/scale-ms/issues/89
    # See also https://github.com/SCALE-MS/scale-ms/issues/90
    # TODO: Set module variables rather than carry around an args namespace?

    _parser.add_argument(
        "--access",
        type=str,
        help="Explicitly specify the access_schema to use from the RADICAL resource.",
    )

    _parser.add_argument(
        "--enable-raptor",
        action="store_true",
        help="Enable RP Raptor, and manage an execution side dispatching task.",
    )

    _parser.add_argument(
        "--pilot-option",
        action="append",
        type=_parse_option,
        metavar="<key>=<value>",
        help="Add a key value pair to the `radical.pilot.PilotDescription`.",
    )

    _parser.add_argument(
        "--resource",
        type=str,
        required=True,
        help="Specify a `RP resource` for the radical.pilot.PilotDescription. (Required)",
    )

    _parser.add_argument(
        "--venv",
        metavar="PATH",
        type=str,
        required=True,
        help="Path to the (pre-configured) Python virtual "
        "environment with which RP tasks should be executed. "
        "(Required. See also https://github.com/SCALE-MS/scale-ms/issues/90)",
    )

    return _parser


class RPResourceParams(typing.TypedDict):
    PilotDescription: dict
    """A dictionary representation of a PilotDescription.

    See :py:class:`radical.pilot.PilotDescription` for allowed fields and value types.
    """


@dataclasses.dataclass(frozen=True)
class RuntimeConfiguration:
    """Module configuration information.

    See also:
        * :py:func:`scalems.radical.configuration`
        * :py:data:`scalems.radical.runtime.parser`
        * :py:class:`scalems.radical.runtime.RuntimeSession`

    .. todo:: Consider merging with module RuntimeSession state container.

    """

    # Note that the use cases for this dataclass interact with module ContextVars,
    # pending refinement.
    datastore: FileStore = None

    execution_target: str = "local.localhost"
    """Platform identifier for the RADCIAL Pilot execution resource."""

    rp_resource_params: RPResourceParams = dataclasses.field(default_factory=dict)
    """Schema for this member container may not be stable."""

    target_venv: str = None
    """Path to a pre-configured Python virtual environment on *execution_target*."""

    enable_raptor: bool = False

    def __post_init__(self):
        hpc_platform_label = self.execution_target
        access = self.rp_resource_params["PilotDescription"].get("access_schema")
        try:
            job_endpoint: ru.Url = rp.utils.misc.get_resource_job_url(hpc_platform_label, schema=access)
        except (TypeError, KeyError) as e:
            raise RPConfigurationError(f"Could not resolve {access} access for {hpc_platform_label}") from e

        if self.enable_raptor:
            # scalems uses the RP MPIWorker, which can have problems in "local" execution modes.
            launch_scheme = job_endpoint.scheme
            if launch_scheme == "fork":
                message = f"RP Raptor MPI Worker not supported for '{launch_scheme}' launch method."
                message += f" '{access}' access for {hpc_platform_label}: {job_endpoint}"
                raise RPConfigurationError(message)


class RuntimeSession:
    """Container for scalems.radical runtime state data.

    Use a creation function to provide RuntimeSession with an asyncio event
    loop. Interact with the RuntimeSession in the main thread whenever
    possible. Let the RuntimeSession dispatch slow rp UI calls to other
    threads as needed and appropriate.

    Note:
        There is very little automated error recovery. For examples of expansive
        checking and re-launching of runtime resources, refer to the fixtures
        in conftest.py at or before revision 41b965a27c5af9abc115677b738085c35766b5b6.
    """

    raptor: typing.Optional[rp.Task] = None
    """The active raptor scheduler task, if any."""

    resources: typing.Optional[asyncio.Task[dict]] = None
    """The active Pilot resources, if any.

    The runtime_startup routine schedules a Task to get a copy of
    the Pilot.resource_details['rm_info'] dictionary, once the Pilot
    reaches state PMGR_ACTIVE.
    """

    _configuration: RuntimeConfiguration
    _loop: asyncio.AbstractEventLoop
    _pilot_manager: typing.Optional[rp.PilotManager] = None
    _pilot: typing.Optional[rp.Pilot] = None
    _session: rp.Session
    _task_manager: typing.Optional[rp.TaskManager] = None

    def __init__(self, session: rp.Session, *, loop: asyncio.AbstractEventLoop, configuration: RuntimeConfiguration):
        if not isinstance(session, rp.Session) or session.closed:
            raise ValueError("*session* must be an active RADICAL Pilot Session.")
        self._session = session
        self._session_finalizer = weakref.finalize(self, session.close)
        if loop.is_closed():
            raise ValueError("*loop* must be an active event loop.")
        # Note: loop.is_running() may not yet return True if no coroutines have been awaited.
        self._loop = loop
        self._configuration = configuration

    def __repr__(self):
        if session := self._session:
            session = session.uid
        if pilot := self._pilot:
            pilot = pilot.uid
        raptor_id = getattr(self.raptor, "uid", None)
        representation = f'<RuntimeSession "{session}" pilot:"{pilot}" raptor:"{raptor_id}">'
        return representation

    async def wait_closed(self):
        """Wait for a closing session to be closed.

        Use with `close()` to allow the asyncio event loop to resolve outstanding tasks.

        TODO: Do we need this?
        This method may not be necessary. Or it may be more necessary in the future.
        """
        while not self.resources.done():
            timer = asyncio.create_task(asyncio.sleep(10.0), name="Session closing timer")
            done, pending = asyncio.wait((self.resources, timer), return_when=asyncio.FIRST_EXCEPTION)
            if self.resources in done:
                timer.cancel()
            else:
                logger.info("Waiting for session to close.")

    def close(self):
        """Direct the runtime to shut down and release resources.

        Warning:
            This function may return before resources have been finalized.
            Follow a call to `close()` with `wait_closed()` to give the event
            loop a chance to cycle.
        """

        # De-initialize state: reset data members to class defaults.

        if self.resources is not None and not self.resources.done():
            if threading.main_thread() == threading.current_thread():
                self.resources.cancel()
            else:
                self._loop.call_soon_threadsafe(self.resources.cancel)

        # TODO: Properly close raptor session. This includes sending a "stop" to the
        #     raptor scheduler instead of letting it get a `Task.cancel()` (i.e. a SIGTERM).
        if self.raptor is not None:
            del self.raptor

        if self._pilot is not None:
            del self._pilot
        if self._task_manager is not None:
            del self._task_manager
        if self._pilot_manager is not None:
            del self._pilot_manager

        # Note: there are no documented exceptions or errors to check for,
        # programmatically. Some issues encountered during shutdown will be
        # reported through the reporter or logger of the
        # radical.pilot.utils.component.Component base.
        # The RP convention seems to be to use the component uid as the name
        # of the underlying logging.Logger node, so we could presumably attach
        # a log handler to the logger for a component of interest.
        logger.debug(f"Closing Session {self.session.uid}.")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")
            self.session.close(download=True)

        self._session_finalizer()
        del self._session_finalizer

    @property
    def session(self) -> rp.Session:
        """The current radical.pilot.Session (may already be closed)."""
        return self._session

    def _new_pilotmanager(self):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

            return rp.PilotManager(session=self.session)

    @typing.overload
    def pilot_manager(self) -> typing.Union[rp.PilotManager, None]:
        ...

    @typing.overload
    def pilot_manager(self, pilot_manager: str) -> rp.PilotManager:
        ...

    @typing.overload
    def pilot_manager(self, pilot_manager: rp.PilotManager) -> rp.PilotManager:
        ...

    def pilot_manager(self, pilot_manager=None) -> typing.Union[rp.PilotManager, None]:
        """Get (optionally set) the current PilotManager.

        Args:
            pilot_manager (optional, radical.pilot.PilotManager, str):
                Set to RP PilotManager instance or identifier, if provided.

        Returns:
            radical.pilot.PilotManager: instance, if set, else ``None``.

        Raises:
            ValueError: for invalid identifier.
            APIError: for invalid RP Session configuration.
        """
        if pilot_manager is None:
            # Caller should destroy and recreate Pilot if this call has to replace PilotManager.
            session = self.session
            if session.closed:
                # Once rp.Session is closed, require a new RuntimeSession.
                raise ProtocolError(f"RP Session {self.session.uid} is closed. Get a new RuntimeSession instance.")
            if self._pilot_manager is not None:
                return self._pilot_manager
            # Is there a way to check whether the PilotManager is healthy?
            logger.info(f"Creating a new PilotManager for {self.session.uid}")
            manager = self._new_pilotmanager()
            logger.info(f"New PilotManager is {manager.uid}")
            return self.pilot_manager(manager)
        elif isinstance(pilot_manager, rp.PilotManager):
            if self._pilot_manager is not None and pilot_manager != self._pilot_manager:
                raise APIError(f"PilotManager {self._pilot_manager.uid} already assigned.")
            if not pilot_manager.session.uid == self.session.uid:
                raise APIError("Cannot accept a PilotManager from a different Session.")
            self._pilot_manager = pilot_manager
            return self._pilot_manager
        else:
            uid = pilot_manager
            try:
                pmgr = self.session.get_pilot_managers(pmgr_uids=uid)
                assert isinstance(pmgr, rp.PilotManager)
            except (AssertionError, KeyError) as e:
                raise ValueError(f"{uid} does not describe a valid PilotManager") from e
            except Exception as e:
                logger.exception("Unhandled RADICAL Pilot exception.", exc_info=e)
                raise ValueError(f"{uid} does not describe a valid PilotManager") from e
            else:
                return self.pilot_manager(pmgr)

    @typing.overload
    def task_manager(self) -> typing.Union[rp.TaskManager, None]:
        ...

    @typing.overload
    def task_manager(self, task_manager: str) -> rp.TaskManager:
        ...

    @typing.overload
    def task_manager(self, task_manager: rp.TaskManager) -> rp.TaskManager:
        ...

    def task_manager(self, task_manager=None) -> typing.Union[rp.TaskManager, None]:
        """Get (optionally set) the current TaskManager.

        Args:
            task_manager (optional, radical.pilot.TaskManager, str):
                Set to RP TaskManager instance or identifier, if provided.

        Returns:
            radical.pilot.TaskManager: instance, if set, else ``None``.

        Raises:
            ValueError: for invalid identifier.
            APIError: for invalid RP Session configuration.
        """
        if task_manager is None:
            return self._task_manager
        elif isinstance(task_manager, rp.TaskManager):
            if not task_manager.session.uid == self.session.uid:
                raise APIError("Cannot accept a TaskManager from a different Session.")
            self._task_manager = task_manager
            return task_manager
        else:
            uid = task_manager
            try:
                tmgr = self.session.get_task_managers(tmgr_uids=uid)
                assert isinstance(tmgr, rp.TaskManager)
            except (AssertionError, KeyError) as e:
                raise ValueError(f"{uid} does not describe a valid TaskManager") from e
            except Exception as e:
                logger.exception("Unhandled RADICAL Pilot exception.", exc_info=e)
                raise ValueError(f"{uid} does not describe a valid TaskManager") from e
            else:
                return self.task_manager(tmgr)

    @staticmethod
    def _new_pilot(
        *,
        session: rp.Session,
        pilot_manager: rp.PilotManager,
        pilot_description: rp.PilotDescription,
        task_manager: rp.TaskManager,
    ):
        logger.debug(
            "Using resource config: {}".format(str(session.get_resource_config(pilot_description.resource).as_dict()))
        )
        logger.debug("Using PilotDescription: {}".format(str(pilot_description.as_dict())))
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.task_manager")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.db.database")
            warnings.filterwarnings("ignore", category=DeprecationWarning, module="radical.pilot.session")

            pilot = pilot_manager.submit_pilots([rp.PilotDescription(pilot_description)])[0]
            task_manager.add_pilots(pilot)
        return pilot

    def pilot(self) -> rp.Pilot:
        """Get active Pilot.

        Allows lazy initialization of the Pilot resource.

        Returns:
            radical.pilot.Pilot: The current Pilot instance, if available and valid,
                or a new Pilot instance in the configured PilotManager.

        Raises:
            APIError: for invalid RP Session configuration.
        """
        pilot_manager = self.pilot_manager()
        if not pilot_manager:
            raise APIError("Cannot get/set Pilot before setting PilotManager.")

        pilot = self._pilot

        if pilot is None or pilot.state in rp.FINAL:
            if pilot is None:
                logger.info(f"Creating a Pilot for {self.session.uid}")
            else:
                assert isinstance(pilot, rp.Pilot)
                logger.info(f"Old Pilot {pilot.uid} in state {pilot.state}")
            pilot_description = describe_pilot(self._configuration)

            logger.debug("Requesting Pilot: {}".format(repr(pilot_description.as_dict())))
            task_manager = self.task_manager()

            pilot = self._new_pilot(
                session=self.session,
                pilot_manager=pilot_manager,
                pilot_description=pilot_description,
                task_manager=task_manager,
            )
            logger.debug(f"Got Pilot {pilot.uid}: {pilot.as_dict()}")

            # Note: This could take hours or days depending on the queuing system.
            # Can we report some more useful information, like job ID?
            # self.resources = self._loop.create_task(pilot_resources(pilot), name="Pilot resources")
            self.resources = asyncio.create_task(get_pilot_resources(pilot), name="Pilot resources")

            self._pilot = pilot
        # Do some checking.
        session = pilot.session
        assert isinstance(session, rp.Session)
        if session.uid != self.session.uid:
            raise APIError("Cannot accept a Pilot from a different Session.")
        if pilot.pmgr.uid != pilot_manager.uid:
            raise APIError("Pilot must be associated with a PilotManager already configured.")

        return pilot


def describe_pilot(configuration: RuntimeConfiguration):
    pilot_description_dict = configuration.rp_resource_params["PilotDescription"].copy()
    # Get a unique identifier.
    pilot_description_dict["uid"] = f"pilot.{str(uuid.uuid4())}"
    pilot_description_dict["resource"] = configuration.execution_target
    assert pilot_description_dict["exit_on_error"] is False
    # if pilot_description_dict.get("exit_on_error", True):
    #     warnings.warn("Failing to set PilotDescription.exit_on_error to False may prevent clean shut down.")
    pilot_description = rp.PilotDescription(pilot_description_dict)
    return pilot_description


async def _get_scheduler(
    pre_exec: typing.Iterable[str],
    task_manager: rp.TaskManager,
    filestore: FileStore,
    scalems_env: str,
):
    """Establish the radical.pilot.raptor.Master task.

    Create a raptor rp.Task (running the scalems_rp_master script) with the
    provided *name* to be referenced as the *scheduler* for raptor tasks.

    Returns the rp.Task for the raptor script once the Master is ready to
    receive submissions.

    Raises:
        DispatchError if the raptor task could not be launched successfully.

    Note:
        Currently there is no completion condition for the raptor script.
        Caller is responsible for canceling the Task returned by this function.
    """
    # define a raptor.scalems raptor and launch it within the pilot
    td = rp.TaskDescription()

    # The raptor uid is used as the `scheduler` value for raptor task routing.
    # TODO(#108): Use caller-provided *name* for master_identity.
    # Master tasks may not appear unique, but must be uniquely identified within the
    # scope of a rp.Session for RP bookkeeping. Since there is no other interesting
    # information at this time, we can generate a random ID and track it in our metadata.
    master_identity = EphemeralIdentifier()
    td.uid = "scalems-rp-raptor." + str(master_identity)

    td.mode = rp.RAPTOR_MASTER

    # scalems_rp_master will write output before it begins handling requests. The
    # script may crash even before it can write anything, but if it does write
    # anything, we _will_ have the output file locally.
    # TODO: Why don't we have the output files? Can we ensure output files for CANCEL?
    td.output_staging = []  # TODO(#229) Write and stage output from raptor task.
    td.stage_on_error = True

    td.pre_exec = list(pre_exec)

    # We are not using prepare_env at this point. We use the `venv` configured by the
    # caller.
    # td.named_env = 'scalems_env'

    td.executable = "python3"
    td.arguments = []
    if os.getenv("PYTHONDEVMODE", None) == "1" or "dev" in getattr(sys, "_xoptions", {}):
        td.arguments.extend(("-X", "dev"))
    if os.getenv("COVERAGE_RUN") is not None or os.getenv("SCALEMS_COVERAGE") is not None:
        # TODO: Use the FileStore!
        local_coverage_dir = pathlib.Path("scalems-remote-coverage-dir").resolve()
        td.arguments.extend(
            (
                "-m",
                "coverage",
                "run",
                "--parallel-mode",
                "--source=scalems",
                "--branch",
                "--data-file=coverage_dir/.coverage",
            )
        )
        td.environment["SCALEMS_COVERAGE"] = "TRUE"
        td.environment["RADICAL_LOG_LVL"] = "DEBUG"
        td.pre_exec.append("mkdir -p coverage_dir")
        td.output_staging.extend(
            (
                {
                    "source": "task:///coverage_dir",
                    "target": local_coverage_dir.as_uri(),
                    "action": rp.TRANSFER,
                },
            )
        )
        logger.info(f"Coverage data will be staged to {local_coverage_dir}.")
    else:
        logger.debug("No coverage testing")
    td.arguments.extend(("-m", "scalems.radical.raptor"))

    logger.debug(f"Using {filestore}.")

    # _original_callback_duration = asyncio.get_running_loop().slow_callback_duration
    # asyncio.get_running_loop().slow_callback_duration = 0.5
    config_file = await asyncio.create_task(
        raptor_input(filestore=filestore, worker_pre_exec=list(pre_exec), worker_venv=scalems_env),
        name="get-raptor-input",
    )
    # asyncio.get_running_loop().slow_callback_duration = _original_callback_duration

    # TODO(#75): Automate handling of file staging directives for scalems.file.AbstractFileReference
    # e.g. _add_file_dependency(td, config_file)
    config_file_name = str(td.uid) + "-config.json"
    td.input_staging = [
        {
            "source": config_file.as_uri(),
            "target": f"task:///{config_file_name}",
            "action": rp.TRANSFER,
        }
    ]
    td.arguments.append(config_file_name)

    task_metadata = {"uid": td.uid, "task_manager": task_manager.uid}

    await asyncio.create_task(asyncio.to_thread(filestore.add_task, master_identity, **task_metadata), name="add-task")
    # filestore.add_task(master_identity, **task_metadata)

    logger.debug(f"Launching RP raptor scheduling. Submitting {td}.")

    _task = asyncio.create_task(asyncio.to_thread(task_manager.submit_tasks, td), name="submit-Master")
    raptor: rp.Task = await _task

    # WARNING: rp.Task.wait() *state* parameter does not handle tuples, but does not
    # check type.
    _task = asyncio.create_task(
        asyncio.to_thread(raptor.wait, state=[rp.states.AGENT_EXECUTING] + rp.FINAL),
        name="check-Master-started",
    )
    await _task
    logger.debug(f"Scheduler in state {raptor.state}.")
    # TODO: Generalize the exit status checker for the Master task and perform this
    #  this check at the call site.
    if raptor.state in rp.FINAL:
        if raptor.stdout or raptor.stderr:
            logger.error(f"raptor.stdout: {raptor.stdout}")
            logger.error(f"raptor.stderr: {raptor.stderr}")
        raise DispatchError(f"Master Task unexpectedly reached {raptor.state} during launch.")
    return raptor


# functools can't cache this function while Configuration is unhashable (due to
# unhashable dict member).
# @functools.cache
def get_pre_exec(conf: RuntimeConfiguration) -> tuple:
    """Get the sequence of pre_exec commands for tasks on the currently configured execution target.

    Warning:
        Use cases may require a `list` object. Caller is responsible for converting
        the returned tuple if appropriate.

    """
    if conf.target_venv is None or len(conf.target_venv) == 0:
        raise ValueError("Currently, tasks cannot be dispatched without a target venv.")

    activate_venv = ". " + str(os.path.join(conf.target_venv, "bin", "activate"))
    # Note: RP may specifically expect a `list` and not a `tuple`.
    sequence = (activate_venv,)
    return sequence


@functools.singledispatch
def _set_configuration(*args, **kwargs) -> RuntimeConfiguration:
    """Initialize or retrieve the module configuration.

    This module and the RADICAL infrastructure have various stateful aspects
    that require clearly-scoped module-level configuration. Module configuration
    should be initialized exactly once per Python process.

    Recommended usage is to derive an ArgumentParser from the *parser()* module
    function and use the resulting namespace to initialize the module configuration
    using this function.

    Note this is a dispatch function. "Overloads" are defined in separate decorated
    functions for calls that provide a `Configuration` or `argparse.Namespace`
    object as the first (and only) positional argument.
    """
    assert len(args) != 0 or len(kwargs) != 0
    # Caller has provided arguments.
    # Not thread-safe
    # FIX: Only the RuntimeManager context manager will manipulate this context variable
    if _configuration.get(None):
        raise APIError(f"configuration() cannot accept arguments when {__name__} is already configured.")
    c = RuntimeConfiguration(*args, **kwargs)
    # _configuration.set(c)
    # return _configuration.get()
    return c


@_set_configuration.register
def _(config: RuntimeConfiguration) -> RuntimeConfiguration:
    # Not thread-safe
    if _configuration.get(None):
        raise APIError(f"configuration() cannot accept arguments when {__name__} is already configured.")
    _configuration.set(config)
    return _configuration.get()


@_set_configuration.register
def _(namespace: argparse.Namespace) -> RuntimeConfiguration:
    rp_resource_params = {
        "PilotDescription": {
            "access_schema": namespace.access,
            "exit_on_error": False,
        }
    }
    if namespace.pilot_option is not None and len(namespace.pilot_option) > 0:
        user_options = _PilotDescriptionProxy.normalize_values(namespace.pilot_option)
        rp_resource_params["PilotDescription"].update(user_options)
        logger.debug(f'Pilot options: {repr(rp_resource_params["PilotDescription"])}')

    if namespace.enable_raptor:
        logger.debug("RP Raptor enabled.")
    else:
        logger.debug("RP Raptor disabled.")

    config = RuntimeConfiguration(
        execution_target=namespace.resource,
        target_venv=namespace.venv,
        rp_resource_params=rp_resource_params,
        enable_raptor=namespace.enable_raptor,
    )
    return _set_configuration(config)


def _rp_session(*args, **kwargs) -> rp.Session:
    # Note: radical.pilot.Session creation causes several deprecation warnings.
    # Ref https://github.com/radical-cybertools/radical.pilot/issues/2185
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        # This would be a good time to `await`, if an event-loop friendly
        # Session creation function becomes available.
        session = rp.Session(*args, **kwargs)
        logger.info(f"Created {session.uid}")
    return session


async def runtime_session(*, configuration: RuntimeConfiguration, loop=None) -> RuntimeSession:
    """Start a new RADICAL Pilot Session.

    Returns:
        RuntimeSession instance.

    """
    if loop is None:
        loop = asyncio.get_running_loop()
    _task = asyncio.create_task(asyncio.to_thread(_rp_session), name="create-Session")
    session: rp.Session = await _task
    runtime = RuntimeSession(session=session, loop=loop, configuration=configuration)

    # At some point soon, we need to track Session ID for the workflow metadata.
    session_id = runtime.session.uid
    # Do we want to log this somewhere?
    # session_config = copy.deepcopy(self.session.cfg.as_dict())
    logger.debug("Acquired RP Session {}".format(session_id))

    logger.debug("Launching PilotManager.")
    pilot_manager = await asyncio.create_task(
        asyncio.to_thread(rp.PilotManager, session=runtime.session),
        name="get-PilotManager",
    )
    pilot_manager = runtime.pilot_manager(pilot_manager)
    logger.debug("Got PilotManager {}.".format(pilot_manager.uid))

    logger.debug("Launching TaskManager.")
    task_manager = await asyncio.create_task(
        asyncio.to_thread(rp.TaskManager, session=runtime.session),
        name="get-TaskManager",
    )
    task_manager = runtime.task_manager(task_manager)
    logger.debug(("Got TaskManager {}".format(task_manager.uid)))

    #
    # Get a Pilot
    #
    # We can launch an initial Pilot, but we may have to run further Pilots
    # during self._queue_runner_task (or while servicing scalems.wait() within the
    # with block) to handle dynamic work load requirements.
    # Optionally, we could refrain from launching the pilot here, at all,
    # but it seems like a good chance to start bootstrapping the agent environment.
    #
    # How and when should we update the pilot description?

    pilot = runtime.pilot()
    logger.debug("Added Pilot {} to task manager {}.".format(pilot.uid, runtime.task_manager().uid))

    return runtime


@functools.singledispatch
def normalize(hint: object, value):
    """Dispatching value normalizer.

    Normalize value according to hint.

    Raises:
        MissingImplementationError: if key could not be dispatched for normalization.
        TypeError: if value could not be normalized according to hint.

    """
    raise MissingImplementationError(f"No dispatcher for {repr(value)} -> {repr(hint)}.")


@normalize.register
def _(hint: type, value):
    return hint(value)


@normalize.register
def _(hint: list, value):
    if len(hint) != 1:
        raise InternalError(f"Expected a list of one type element. Got {repr(hint)}.")
    if isinstance(value, (str, bytes)) or not isinstance(value, collections.abc.Iterable):
        raise TypeError(f"Expected a list-like value. Got {repr(value)}.")
    return [normalize(hint[0], element) for element in value]


@normalize.register
def _(hint: dict, value):
    try:
        for key in value.keys():
            if key not in hint:
                raise MissingImplementationError(f"{key} is not a valid field.")
        items: tuple = value.items()
    except AttributeError:
        raise TypeError(f"Expected a dict-like value. Got {repr(value)}.")
    return {key: normalize(hint[key], val) for key, val in items}


class _PilotDescriptionProxy(rp.PilotDescription):
    """Use PilotDescription details to normalize the value types of description fields."""

    assert hasattr(rp.PilotDescription, "_schema")
    assert isinstance(rp.PilotDescription._schema, dict)
    assert all(
        map(
            lambda v: isinstance(v, (type, list, dict, type(None))),
            rp.PilotDescription._schema.values(),
        )
    )

    @classmethod
    def normalize_values(cls, desc: typing.Sequence[tuple]):
        """Generate normalized key-value tuples.

        For values that are not already of the appropriate type, cast according to
        PilotDescription._schema.

        Args:
            desc: sequence of key-value tuples for PilotDescription fields.

        Raises:
            MissingImplementationError: if key could not be dispatched for normalization.
            TypeError: if value could not be normalized according to hint.

        """
        for key, value in desc:
            try:
                hint = cls._schema[key]
            except KeyError:
                raise MissingImplementationError(f"{key} is not a valid PilotDescription field.")
            if not isinstance(hint, type):
                # This may be overly aggressive, but at the moment we are only normalizing values from
                # the command line parser, and we don't have a good way to pre-parse list or dict values.
                raise MissingImplementationError(f"No handler for {key} field of type {repr(hint)}.")

            if isinstance(None, hint) or isinstance(value, hint):
                yield key, value
            else:
                yield key, normalize(hint, value)


async def get_pilot_resources(pilot: rp.Pilot):
    def log_pilot_state(fut: asyncio.Task[str]):
        if not fut.cancelled():
            if e := fut.exception():
                logger.exception("Exception while watching for Pilot to become active.", exc_info=e)
        logger.info(f"Pilot {pilot.uid} in state {pilot.state}.")

    logger.info("Waiting for an active Pilot.")
    # Wait for Pilot to be in state PMGR_ACTIVE. (There is no reasonable
    # choice of a timeout because we are waiting for the HPC queuing system.)
    # Then, query Pilot.resource_details['rm_info']['requested_cores'] and 'requested_gpus'.
    pilot_state = asyncio.create_task(
        asyncio.to_thread(pilot.wait, state=rp.PMGR_ACTIVE, timeout=None), name="pilot_state_waiter"
    )

    pilot_state.add_done_callback(log_pilot_state)
    await pilot_state
    rm_info: dict = pilot.resource_details.get("rm_info")
    logger.debug(f"Pilot {pilot.uid} resources: {str(rm_info)}")
    if rm_info is not None:
        assert isinstance(rm_info, dict)
        assert "requested_cores" in rm_info and isinstance(rm_info["requested_cores"], int)
        assert "requested_gpus" in rm_info and isinstance(rm_info["requested_gpus"], typing.SupportsFloat)
        return rm_info.copy()


class RPDispatchingExecutor(scalems.execution.RuntimeManager[RuntimeConfiguration]):
    """Client side manager for work dispatched through RADICAL Pilot.

    Extends :py:class:`scalems.execution.RuntimeManager`

    Configuration points:

    * resource config
    * pilot config
    * session config?

    We try to wrap rp UI calls in separate threads. Note, though, that

    * The rp.Session needs to be created in the root thread to be able to correctly
      manage signal handlers and subprocesses, and
    * We need to be able to schedule RP Task callbacks in the same process as the
      asyncio event loop in order to handle Futures for RP tasks.

    See https://github.com/SCALE-MS/randowtal/issues/2
    """

    runtime: "scalems.radical.runtime.RuntimeSession"
    """See `scalems.execution.RuntimeManager.runtime`"""

    def __init__(
        self,
        *,
        editor_factory: typing.Callable[[], typing.Callable] = None,
        datastore: FileStore = None,
        loop: asyncio.AbstractEventLoop,
        configuration: RuntimeConfiguration,
    ):
        """Create a client side execution manager.

        Warning:
            The creation method does not fully initialize the instance.

            Initialization and de-initialization occurs through
            the Python (async) context manager protocol.

        """
        if "RADICAL_PILOT_DBURL" not in os.environ:
            raise DispatchError("RADICAL Pilot environment is not available.")

        if not isinstance(configuration.target_venv, str) or len(configuration.target_venv) == 0:
            raise ValueError("Caller must specify a venv to be activated by the execution agent for dispatched tasks.")

        super().__init__(editor_factory=editor_factory, datastore=datastore, loop=loop, configuration=configuration)

    @contextlib.contextmanager
    def runtime_configuration(self):
        """Provide scoped Configuration.

        Merge the runtime manager's configuration with the global configuration,
        update the global configuration, and yield the configuration for a ``with`` block.

        Restores the previous global configuration when exiting the ``with`` block.

        Warning:
            Not thread-safe.

            We do not check for re-entrance, which will cause race conditions w.r.t.
            which Context state is restored! Moreover, the Configuration object is not
            currently hashable and does not have an equality test defined.

            This contextmanager is not async, but it could be (and is) used within
            an asynchronous context manager, so we don't have anything structurally
            prohibiting reentrant calls, even without multithreading.

        TODO:
            Reconsider logic for runtime module scoped Configuration management.

            Instead of managing the current contextvars.Context,
            this should be used to produce an updated contextvars.Context for use by
            a Task or Context.run() scope. Alternatively, we can decouple the runtime configuration
            from the global module configuration, reduce the dynamic utility of
            *<module>.configuration()*, and not use contextvars.

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
        if self._runtime_configuration.target_venv is not None and len(self._runtime_configuration.target_venv) > 0:
            configuration_dict["target_venv"] = self._runtime_configuration.target_venv
        if len(self._runtime_configuration.rp_resource_params) > 0:
            configuration_dict["rp_resource_params"].update(self._runtime_configuration.rp_resource_params)
        if (
            self._runtime_configuration.execution_target is not None
            and len(self._runtime_configuration.execution_target) > 0
        ):
            configuration_dict["execution_target"] = self._runtime_configuration.execution_target
        configuration_dict["datastore"] = self.datastore

        c = RuntimeConfiguration(**configuration_dict)

        token = _configuration.set(c)
        try:
            yield c
        finally:
            _configuration.reset(token)

    async def runtime_startup(self) -> asyncio.Task:
        """Establish the RP Session.

        Acquire a maximally re-usable set of RP resources. The scope established by
        this function is as broad as it can be within the life of the workflow manager.

        Once *instance.runtime_startup()* succeeds, *instance.runtime_shutdown()*
        must be called to clean up resources.
        Use the async context manager behavior of the instance to
        automatically follow this protocol. I.e. instead of calling
        ``instance.runtime_startup(); ...; instance.runtime_shutdown()``,
        use::

            async with instance:
                ...

        Raises:
            DispatchError: if task dispatching could not be set up.
            asyncio.CancelledError: if parent `asyncio.Task` is cancelled while executing.

        Note:
            **Signal handling**

            RP is known to use IPC signals in several cases. We believe that the
            client environment should only experience an internally triggered
            SIGINT in a single code path, and the behavior can be suppressed by
            setting the Pilot's :py:attr:`~radical.pilot.PilotDescription.exit_on_error`
            attribute to `False`.

            We could use
            `loop.add_signal_handler()
            <https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.add_signal_handler>`__
            to convert to an exception that we can raise in an appropriate task, but this
            is probably unnecessary. Moreover, with Python 3.11, we get a sensible
            signal handling behavior (for SIGINT) with :py:class:`asyncio.Runner`.
            Per https://docs.python.org/3/library/asyncio-runner.html#handling-keyboard-interruption,
            we can just make sure that our run time resources will be properly
            shut down in the event of a :py:class:`asyncio.CancelledError`, including
            sending appropriate calls to the `radical.pilot` framework.

            See Also:
                https://github.com/SCALE-MS/randowtal/issues/1

        TODO: More concurrency.
            The rp.Pilot and raptor task can be separately awaited, and we should allow
            input data staging to begin as soon as we have enough run time details to do it.
            We need to clarify which tasks should be in which state to consider the
            asynchronous context manager to have been successfully "entered". I expect
            that success includes a Pilot in state PMGR_ACTIVE, and a raptor Task in
            state AGENT_EXECUTING, and asyncio.Task handles available for other aspects,
            like synchronization of metadata and initiation of input data staging.
            However, we may prefer that the workflow script can continue evaluation on
            the client side while waiting for the Pilot job, and that we avoid blocking
            until we absolutely have to (presumably when exiting the dispatching context).

        """
        config: RuntimeConfiguration = configuration()

        # TODO: Check that we have a FileStore.

        try:
            #
            # Start the Session.
            #

            # Note: the current implementation implies that only one Task for the dispatcher
            # will exist at a time. We are further assuming that there will probably only
            # be one Task per the lifetime of the dispatcher object.
            # We could choose another approach and change our assumptions, if appropriate.
            logger.debug("Entering RP dispatching context. Waiting for rp.Session.")

            _runtime: RuntimeSession = await runtime_session(loop=self._loop, configuration=config)

            # TODO: Asynchronous data staging optimization.
            #  Allow input data staging to begin before scheduler is in state EXECUTING and
            #  before Pilot is in state PMGR_ACTIVE.

            assert _runtime.raptor is None

            # Get a scheduler task IFF raptor is explicitly enabled.
            if config.enable_raptor:
                task_manager = _runtime.task_manager()
                # Note that _get_scheduler is a coroutine that, itself, returns a rp.Task.
                # We await the result of _get_scheduler, then store the scheduler Task.
                _runtime.raptor = await asyncio.create_task(
                    _get_scheduler(
                        pre_exec=list(get_pre_exec(config)),
                        task_manager=task_manager,
                        filestore=config.datastore,
                        scalems_env="scalems_venv",
                        # TODO: normalize ownership of this name.
                    ),
                    name="get-scheduler",
                )  # Note that we can derive scheduler_name from self.scheduler.uid in later methods.
        except asyncio.CancelledError as e:
            raise e
        except Exception as e:
            logger.exception("Exception while connecting RADICAL Pilot.", exc_info=e)
            raise DispatchError("Failed to launch SCALE-MS raptor task.") from e

        self.runtime = _runtime

        if self.runtime is None or self.runtime.session.closed:
            raise ProtocolError("Cannot process queue without a RP Session.")

        # Launch queue processor (proxy executor).
        # TODO: Make runtime_startup optional. Let it return a resource that is
        #  provided to the normalized run_executor(), or maybe use it to configure the
        #  Submitter that will be provided to the run_executor.
        runner_started = asyncio.Event()
        runner_task = asyncio.create_task(scalems.execution.manage_execution(self, processing_state=runner_started))
        await runner_started.wait()
        # TODO: Note the expected scope of the runner_task lifetime with respect to
        #  the global state changes (i.e. ContextVars and locks).
        return runner_task

    @staticmethod
    async def cpi(command: str, runtime: RuntimeSession):
        """Send a control command to the raptor scheduler.

        Implements :py:func:`scalems.execution.RuntimeManager.cpi()`

        TODO: Unify with new raptor cpi feature.
        """
        timeout = 180

        logger.debug(f'Received command "{command}" for runtime {runtime}.')
        if runtime is None:
            raise scalems.exceptions.ScopeError("Cannot issue control commands without an active RuntimeManager.")
        raptor: rp.Task = runtime.raptor
        logger.debug(f"Preparing command for {repr(raptor)}.")
        if raptor is None:
            raise scalems.exceptions.ScopeError(
                f"Cannot issue control commands without an active RuntimeManager. {runtime}"
            )
        if raptor.state in rp.FINAL:
            raise scalems.exceptions.ScopeError(f"Raptor scheduler is not available. {repr(raptor)}")
        assert raptor.uid
        message = scalems.messages.Control.create(command)
        td = rp.TaskDescription(
            from_dict={
                "raptor_id": raptor.uid,
                "mode": scalems.radical.raptor.CPI_MESSAGE,
                "metadata": message.encode(),
                "uid": EphemeralIdentifier(),
            }
        )
        logger.debug(f"Submitting {str(td.as_dict())}")
        (task,) = await asyncio.to_thread(runtime.task_manager().submit_tasks, [td])
        logger.debug(f"Submitted {str(task.as_dict())}. Waiting...")
        # Warning: we can't wait on the final state of such an rp.Task unless we
        # _also_ watch the scheduler itself, because there are various circumstances
        # in which the Task may never reach a rp.FINAL state.

        command_watcher = asyncio.create_task(
            asyncio.to_thread(task.wait, state=rp.FINAL, timeout=timeout), name="cpi-watcher"
        )

        raptor_watcher = asyncio.create_task(
            asyncio.to_thread(raptor.wait, state=rp.FINAL, timeout=timeout), name="raptor-watcher"
        )
        # If raptor task fails, command-watcher will never complete.
        done, pending = await asyncio.wait(
            (command_watcher, raptor_watcher), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if raptor_watcher in done and command_watcher in pending:
            command_watcher.cancel()
        logger.debug(str(task.as_dict()))
        # WARNING: Dropping the Task reference will cause its deletion.
        return command_watcher

    def runtime_shutdown(self, runtime: RuntimeSession):
        """Manage tear down of the RADICAL Pilot Session and resources.

        Several aspects of the RP runtime interface use blocking calls.
        This method should be run in a non-root thread (concurrent.futures.Future)
        that the event loop can manage as an asyncio-wrapped task.

        Overrides :py:class:`scalems.execution.RuntimeManager`
        """
        # TODO: Move this to a RuntimeSession.close() method.
        session: rp.Session = getattr(runtime, "session", None)
        if session is None:
            raise scalems.exceptions.APIError(f"No Session in {runtime}.")
        if session.closed:
            logger.error("RuntimeSession is already closed?!")
        else:
            if runtime.raptor is not None:
                # Note: The __aexit__ for the RuntimeManager makes sure that a `stop`
                # is issued after the work queue is drained, if the scheduler task has
                # not already ended. We could check the status of this stop message...
                if runtime.raptor.state not in rp.FINAL:
                    logger.info(f"Waiting for RP Raptor raptor task {runtime.raptor.uid} to complete...")
                    runtime.raptor.wait(rp.FINAL, timeout=60)
                if runtime.raptor.state not in rp.FINAL:
                    # Cancel the raptor.
                    logger.warning("Canceling the raptor scheduling task.")
                    # Note: the effect of CANCEL is to send SIGTERM to the shell that
                    # launched the task process.
                    # TODO: Report incomplete tasks and handle the consequences of terminating the
                    #  Raptor processes early.
                    task_manager = runtime.task_manager()
                    task_manager.cancel_tasks(uids=runtime.raptor.uid)
                # As of https://github.com/radical-cybertools/radical.pilot/pull/2702,
                # we do not expect `cancel` to block, so we must wait for the
                # cancellation to succeed. It shouldn't take long, but it is not
                # instantaneous or synchronous. We hope that a minute is enough.
                final_state = runtime.raptor.wait(state=rp.FINAL, timeout=10)
                logger.debug(f"Final state: {final_state}")
                logger.info(f"Master scheduling task state {runtime.raptor.state}: {repr(runtime.raptor)}.")
                if runtime.raptor.stdout:
                    # TODO(#229): Fetch actual stdout file.
                    logger.debug(runtime.raptor.stdout)
                if runtime.raptor.stderr:
                    # TODO(#229): Fetch actual stderr file.
                    # Note that all of the logging output goes to stderr, it seems,
                    # so we shouldn't necessarily consider it an error level event.
                    logger.debug(runtime.raptor.stderr)  # TODO(#108,#229): Receive report of work handled by Master.

            runtime.close()

            if session.closed:
                logger.debug(f"Session {session.uid} closed.")
            else:
                logger.error(f"Session {session.uid} not closed!")
        logger.debug("RuntimeSession shut down.")

    def updater(self) -> "WorkflowUpdater":
        return WorkflowUpdater(executor=self)


def configuration(*args, **kwargs) -> RuntimeConfiguration:
    """Get (and optionally set) the RADICAL runtime configuration.

    With no arguments, returns the current configuration. If a configuration has
    not yet been set, the command line parser is invoked to try to build a new
    configuration.

    If arguments are provided, try to construct a `scalems.radical.runtime.Configuration`
    and use it to initialize the module.

    It is an error to try to initialize the module more than once.
    """
    # Not thread-safe
    from scalems.radical import parser

    if len(args) > 0:
        _set_configuration(*args, **kwargs)
    elif len(kwargs) > 0:
        _set_configuration(RuntimeConfiguration(**kwargs))
    elif _configuration.get(None) is None:
        # No config is set yet. Generate with module parser.
        namespace, _ = parser.parse_known_args()
        _set_configuration(namespace)
    return _configuration.get()


def executor_factory(manager: scalems.workflow.WorkflowManager, params: RuntimeConfiguration = None):
    if params is not None:
        _set_configuration(params)
    params = configuration()

    executor = RPDispatchingExecutor(
        editor_factory=weakref.WeakMethod(manager.edit_item),
        datastore=manager.datastore(),
        loop=manager.loop(),
        configuration=params,
    )
    return executor


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

    def __repr__(self):
        return (
            f"<{self.__class__.__qualname__} "
            f"canceled={self.canceled.is_set()} "
            f"done={self.done.is_set()} "
            f"failed={self.failed.is_set()}>"
        )


def _rp_callback(
    obj: rp.Task,
    state,
    cb_data: weakref.ReferenceType = None,
    final: RPFinalTaskState = None,
):
    """Prototype for RP.Task callback.

    To use, partially bind the *final* parameter (with `functools.partial`) to get a
    callable with the RP.Task callback signature.

    Register with *task* to be called when the rp.Task state changes.
    """
    if final is None:
        raise APIError("This function is strictly for dynamically prepared RP callbacks through " "functools.partial.")
    logger.debug(f"Callback triggered by {repr(obj)} state change to {repr(state)}.")
    try:
        # Note: assertions and exceptions are not useful in RP callbacks.
        if state in (rp.states.DONE, rp.states.CANCELED, rp.states.FAILED):
            # TODO: Pending https://github.com/radical-cybertools/radical.pilot/issues/2444
            # tmgr: rp.TaskManager = obj.tmgr
            # ref = cb_data()
            # tmgr.unregister_callback(cb=ref, metrics='TASK_STATE',
            #                          uid=obj.uid)
            logger.debug(f"Recording final state {state} for {repr(obj)}")
            # Consider alternatives that are easier to await for:
            #   * use an asyncio.Event instead, and use loop.call_soon_threadsafe(<event>.set)
            #   * insert a layer so that the RPFinalTaskState object can easily
            #     set its own awaitable "done" status when updated
            if state == rp.states.DONE:
                final.done.set()
            elif state == rp.states.CANCELED:
                final.canceled.set()
            elif state == rp.states.FAILED:
                final.failed.set()
            else:
                logger.error("Bug: logic error in state cases.")
    except Exception as e:
        logger.error(f"Exception encountered during rp.Task callback: {repr(e)}")


async def _rp_task_watcher(task: rp.Task, final: RPFinalTaskState, ready: asyncio.Event) -> rp.Task:  # noqa: C901
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

            done, pending = await asyncio.wait([event_watcher], timeout=60.0, return_when=asyncio.FIRST_COMPLETED)

            if _rp_task_was_complete and not event_watcher.done():
                event_watcher.cancel()
                raise RPInternalError(
                    f"RP Callbacks are taking too long to complete. Abandoning {repr(task)}. Please report bug."
                )

            if event_watcher in done:
                assert final
                logger.debug(f"Handling finalization for RP task {task.uid}.")
                if final.failed.is_set():
                    logger.error(f"{task.uid} stderr: {task.stderr}")
                    logger.info(f"Failed {task.uid} working directory: {task.task_sandbox}")
                    if logger.getEffectiveLevel() <= logging.DEBUG:
                        for key, value in task.as_dict().items():
                            logger.debug(f"    {key}: {str(value)}")
                    message = f"{task.uid} failed."
                    if task.exit_code is not None:
                        message += f" Exit code {task.exit_code}."
                    if task.stderr is not None:
                        message += f" stderr: {task.stderr}"
                    if task.exception is not None:
                        message += f" Exception: {task.exception}"
                    raise RPTaskFailure(message, task=task)
                elif final.canceled.is_set():
                    # Act as if RP called Task.cancel() on us.
                    raise asyncio.CancelledError()
                assert final.done.is_set()

                logger.debug(f"Publishing results from RP Task {task.uid}.")
                # TODO: Manage result type.
                return task

        raise scalems.exceptions.InternalError("Logic error. This line should not have been reached.")

    except asyncio.CancelledError as e:
        logger.debug(f"Received cancellation in watcher task for {repr(task)}")
        if task.state not in rp.CANCELED:
            logger.debug(f"Propagating cancellation to {repr(task)}.")
            task.cancel()
        raise e


# TODO: Separate this out to a scalems.rptask module.
async def rp_task(rptask: rp.Task) -> asyncio.Task[rp.Task]:
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
    (See :py:func:`_rp_callback()` and :py:class:`RPFinalTaskState`)

    Canceling the returned task will cause *rptask* to be canceled.
    Canceling *rptask* will cause this task to be canceled.

    Arguments:
        rptask: RADICAL Pilot Task that has already been submitted.

    Returns:
        A Task that, when awaited, returns the rp.Task instance in its final state.
    """
    if not isinstance(rptask, rp.Task):
        raise TypeError("Function requires a RADICAL Pilot Task object.")

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
        # radical.pilot.Task.register_callback does not guarantee that callbacks
        # will be called if registered after task completion.
        # Call it once. For simplicity, let the task_watcher logic proceed normally.
        logger.warning(f"RP Task {repr(rptask)} finished suspiciously fast.")
        callback(rptask, rptask.state, cb_data)

    watcher_started = asyncio.Event()
    waiter = asyncio.create_task(watcher_started.wait())
    wrapped_task = asyncio.create_task(_rp_task_watcher(task=rptask, final=final, ready=watcher_started))

    # Make sure that the task is cancellable before returning it to the caller.
    await asyncio.wait((waiter, wrapped_task), return_when=asyncio.FIRST_COMPLETED)
    if wrapped_task.done():
        # Let CancelledError propagate.
        e = wrapped_task.exception()
        if e is not None:
            raise e
    # watcher_task.
    return wrapped_task


def _describe_legacy_task(item: scalems.workflow.Task, pre_exec: list) -> rp.TaskDescription:
    """Derive a RADICAL Pilot TaskDescription from a scalems workflow item.

    For a "raptor" style task, see _describe_raptor_task()
    """
    subprocess_type = TypeIdentifier(("scalems", "subprocess", "SubprocessTask"))
    assert item.description().type() == subprocess_type
    input_data = item.input
    task_input = scalems.subprocess.SubprocessInput(**input_data)
    args = list([arg for arg in task_input.argv])
    # Warning: TaskDescription class does not have a strongly defined interface.
    # Check docs for schema.
    task_description = rp.TaskDescription(
        from_dict=dict(
            executable=args[0],
            arguments=args[1:],
            stdout=str(task_input.stdout),
            stderr=str(task_input.stderr),
            pre_exec=pre_exec,
        )
    )
    uid: str = item.uid().hex()
    task_description.uid = uid

    # TODO: Check for and activate an appropriate venv
    # using
    #     task_description.pre_exec = ...
    # or
    #     task_description.named_env = ...

    # TODO: Interpret item details and derive appropriate staging directives.
    task_description.input_staging = list(task_input.inputs.values())
    task_description.output_staging = [
        {
            "source": str(task_input.stdout),
            "target": os.path.join(uid, pathlib.Path(task_input.stdout).name),
            "action": rp.TRANSFER,
        },
        {
            "source": str(task_input.stderr),
            "target": os.path.join(uid, pathlib.Path(task_input.stderr).name),
            "action": rp.TRANSFER,
        },
    ]
    task_description.output_staging += task_input.outputs.values()

    return task_description


def _describe_raptor_task(item: scalems.workflow.Task, raptor_id: str, pre_exec: list) -> rp.TaskDescription:
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
        from_dict=dict(executable="scalems", pre_exec=pre_exec)
        # This value is currently ignored, but must be set.
    )
    task_description.uid = item.uid()
    task_description.raptor_id = str(raptor_id)
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
        "mode": "exec",
        "cores": 1,
        "timeout": None,
        "data": {"exe": item.input["argv"][0], "args": item.input["argv"][1:]},
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


async def submit(
    *,
    item: scalems.workflow.Task,
    task_manager: rp.TaskManager,
    pre_exec: list,
    raptor_id: typing.Optional[str] = None,
) -> asyncio.Task:
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
        bug in RP. For simplicity, we can just note whether ``rptask.state in rp.FINAL``
        before the watcher goes to sleep and raise an error if the callback is not
        triggered in an iteration where we have set such a flag.

        If our watcher sends a cancellation to the rp.Task, there is no need to
        continue to monitor the rp.Task state and the watcher may exit.

    Args:
        item: The workflow item to be submitted
        task_manager: A `radical.pilot.TaskManager` instance
            through which the task should be submitted.
        pre_exec: :py:data:`radical.pilot.Task.pre_exec` prototype.
        raptor_id (str): The string name of the "scheduler," corresponding to
            the UID of a Task running a rp.raptor.Master (if Raptor is enabled).

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
    def raptor_is_ready(raptor_id):
        return (
            isinstance(raptor_id, str) and len(raptor_id) > 0 and isinstance(task_manager.get_tasks(raptor_id), rp.Task)
        )

    subprocess_type = TypeIdentifier(("scalems", "subprocess", "SubprocessTask"))
    submitted_type = item.description().type()
    if submitted_type == subprocess_type:
        if raptor_id is not None:
            raise DispatchError("Raptor not yet supported for scalems.executable.")
        rp_task_description = _describe_legacy_task(item, pre_exec=pre_exec)
    elif configuration().enable_raptor:
        if raptor_is_ready(raptor_id):
            # We might want a contextvars.Context to hold the current rp.Master instance name.
            rp_task_description = _describe_raptor_task(item, raptor_id, pre_exec=pre_exec)
        else:
            raise APIError("Caller must provide the UID of a submitted *raptor* task.")
    else:
        raise APIError(f"Cannot dispatch {submitted_type}.")

    # TODO(#249): A utility function to move slow blocking RP calls to a separate thread.
    #  Compartmentalize TaskDescription -> rp_task_watcher in a separate utility function.
    (task,) = task_manager.submit_tasks([rp_task_description])

    rp_task_watcher = await rp_task(rptask=task)

    if rp_task_watcher.done():
        if rp_task_watcher.cancelled():
            raise DispatchError(f"Task for {item} was unexpectedly canceled during dispatching.")
        e = rp_task_watcher.exception()
        if e is not None:
            raise DispatchError("Task for {item} failed during dispatching.") from e

    # Warning: in the long run, we should not extend the life of the reference returned
    # by edit_item, and we need to consider the robust way to publish item results.
    # TODO: Translate RP result to item result type.
    rp_task_watcher.add_done_callback(functools.partial(scalems_callback, item=item))
    # TODO: If *item* acquires a `cancel` method, we need to subscribe to it and
    #  respond by unregistering the callback and canceling the future.

    return rp_task_watcher


def scalems_callback(fut: asyncio.Future, *, item: scalems.workflow.Task):
    """Propagate the results of a Future to the subscribed *item*.

    Partially bind *item* to use this as the argument to *fut.add_done_callback()*.

    Warning:
        In the long run, we should not extend the life of the reference returned
        by edit_item, and we need to consider the robust way to publish item results.

    Note:
        This is not currently RP-specific and we should look at how best to factor
        results publishing for workflow items. It may be that this function is the
        appropriate place to convert RP Task results to scalems results.
    """
    assert fut.done()
    if fut.cancelled():
        logger.info(f"Task supporting {item} has been cancelled.")
    else:
        # The following should not throw because we just checked for `done()` and
        # `cancelled()`
        e = fut.exception()
        if e:
            logger.info(f"Task supporting {item} failed: {e}")
        else:
            # TODO: Construct an appropriate scalems Result from the rp Task.
            item.set_result(fut.result())


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
            raise MissingImplementationError("Executor cannot handle multidimensional tasks yet.")

        task: asyncio.Task[rp.Task] = await submit(item=item, task_manager=self.task_manager, pre_exec=self._pre_exec)
        return task


@dataclasses.dataclass
class RPTaskResult:
    """A collection of data and managed objects associated with a completed RP Task.

    A rp.Task is associated with additional artifacts that are not directly tied
    to the rp.Task object, such as an arbitrary number of files that are not generally
    knowable a priori.

    Design note:
        This data structure is intentionally ignorant of wrapped Python functions.
        We should try to reconcile our multiple notions of RP task management as soon
        as possible. We should be careful to distinguish classes for managing traditional
        *executable* RP tasks from Python tasks managed with raptor functionality.
    """

    uid: str
    """:py:attr:`radical.pilot.Task.uid` identifier."""

    task_dict: dict
    """Dictionary representation from :py:func:`radical.pilot.Task.as_dict()`."""

    exit_code: int
    """Exit code of the executable task."""

    final_state: str
    """Final state of the `radical.pilot.Task`."""

    directory: radical.utils.Url
    """Resource location information for the Task working directory.

    Constructed from the filesystem_endpoint. See :doc:`design/localization`.
    """

    directory_archive: Awaitable[scalems.store.FileReference]
    """An archive of the task working directory.

    This result is separately awaitable to allow elision of unnecessary transfers.
    The exact implementation is not strongly specified; a Future usually implies
    that the result will eventually be produced, whereas a more general "awaitable"
    may not and may never be scheduled.

    A future optimization should allow individual files to be selected and remotely
    extracted, but this would require more progress on the Scale-MS structured data typing model.

    TODO: In the next iteration, use a non-local FileReference or (TBD) DirectoryReference.
    """


async def subprocess_to_rp_task(
    call_handle: scalems.call._Subprocess, dispatcher: RPDispatchingExecutor
) -> RPTaskResult:
    """Dispatch a subprocess task through the `scalems.radical` execution backend.

    Get a Future for a RPTaskResult (a collection representing a completed rp.Task).

    Schedule a RP Task and wrap with asyncio. Subscribe a dependent asyncio.Task
    that can provide the intended result type and return it as the Future.
    Schedule a call-back to clean up temporary files.

    Args:
        call_handle (scalems.call._Subprocess): result of a ``await`` on a
            :py:func:`scalems.call.function_call_to_subprocess()`
        dispatcher (RPDispatchingExecutor): An active execution manager.

    TODO:
        This logic should be integrated into a :py:func:`WorkflowManager.submit()`
        stack (currently :py:func:`WorkflowManager.add_item()`)
        and `manage_execution` loop. We really should have special handling for
        wrapped function calls as distinguished from command line executables
        through "overloads" (dispatching) of the task submission.
        Also, we should not be exposing the *dispatcher* to the user.
    """
    subprocess_dict = dict(
        stage_on_error=True,
        uid=call_handle.uid,
        executable=call_handle.executable,
        arguments=list(call_handle.arguments),
        pre_exec=list(get_pre_exec(dispatcher.configuration())),
        mode=rp.TASK_EXECUTABLE,
    )
    if configuration().enable_raptor:
        subprocess_dict["raptor_id"] = dispatcher.runtime.raptor.uid
    # Capturing stdout/stderr is a potentially unusual or unexpected behavior for
    # a Python function runner, and may collide with user assumptions or native
    # behaviors of third party tools. We will specify distinctive names for the RP
    # output capture files in the hope of clarifying the component responsible for
    # these files.
    subprocess_dict["stdout"] = "_scalems_stdout.txt"
    subprocess_dict["stderr"] = "_scalems_stderr.txt"

    # Note: We could save wall time in the job by pre-staging files, but we are
    # deferring that for future optimization.
    # Ref: https://github.com/SCALE-MS/scale-ms/issues/316
    await asyncio.gather(*tuple(ref.localize() for ref in call_handle.input_filenames.values()))
    subprocess_dict["input_staging"] = [
        {
            "source": ref.as_uri(),
            # TODO: Find a programmatic mechanism to translate between URI and CLI arg for robustness.
            "target": f"task:///{name}",
            "action": rp.TRANSFER,
        }
        for name, ref in call_handle.input_filenames.items()
    ]

    # We just pass the user-provided requirements through, but we have to reject
    # any that collide with parameters we expect to generate.
    for param, value in call_handle.requirements.items():
        if param in subprocess_dict:
            raise ValueError(f"Cannot overwrite {param}. Task['{param}'] is {subprocess_dict[param]}")
        else:
            subprocess_dict[param] = value

    try:
        subprocess_task_description = rp.TaskDescription(from_dict=subprocess_dict).verify()
    except radical.utils.typeddict.TDKeyError as e:
        raise ValueError("Invalid attribute for RP TaskDescription.") from e

    task_cores = subprocess_task_description.cores_per_rank * subprocess_task_description.cpu_processes
    rm_info = await dispatcher.runtime.resources
    pilot_cores = rm_info["requested_cores"]
    # TODO: Account for Worker cores.
    if configuration().enable_raptor:
        raptor_task: rp.Task = dispatcher.runtime.raptor
        raptor_cores = raptor_task.description["ranks"] * raptor_task.description["cores_per_rank"]
    else:
        raptor_cores = 0
    if task_cores > (available_cores := pilot_cores - raptor_cores):
        raise ValueError(f"Requested {task_cores} for {call_handle.uid}, but at most {available_cores} are available.")

    # TODO: Find a better way to express these three lines.
    # This seems like it should be a subscription by the local workflow context to the
    # RP dispatching workflow context. Something like
    #     supporting_task = await rp_manager.add_task(task_description)
    #     supporting_task.add_done_callback(...)
    task_manager = dispatcher.runtime.task_manager()
    (submitted_task,) = await asyncio.to_thread(task_manager.submit_tasks, [subprocess_task_description])
    subprocess_task_future: asyncio.Task[rp.Task] = await scalems.radical.runtime.rp_task(submitted_task)

    # TODO: We really should consider putting timeouts on all tasks.
    subprocess_task: rp.Task = await subprocess_task_future

    logger.debug(f"Task {subprocess_task.uid} sandbox: {subprocess_task.task_sandbox}.")

    sandbox: radical.saga.Url = copy.deepcopy(subprocess_task.task_sandbox)
    if not isinstance(sandbox, radical.saga.Url):
        logger.debug(f"Converting {repr(sandbox)} to expected Url type.")
        sandbox = radical.saga.Url(sandbox)

    # TODO: Schedule the task to create the remote archive, separating the archive retrieval.
    #   Get a Future for the saga URL (`done` when the archive exists remotely).
    #   In a later implementation, get a Future[FileReference] that is non-local.
    #   A refactoring of this wrapper can immediately schedule retrieval to support
    #   the packaged result.
    # Note: We need to find language to distinguish a Future that is scheduled from
    #   an awaitable that isn't. The native Coroutine is not sufficient because Coroutines
    #   are expected to be scheduled eventually and to be awaited at some point.
    #   Can we allow all potential outputs to be scheduled, but just at low priority somehow?
    #   Or can this be a formalization of our notion of "localize" as an awaitable that
    #   changes the state of the parent object?
    archive_future = asyncio.create_task(
        get_directory_archive(sandbox, dispatcher=dispatcher),
        name=f"archive-{subprocess_task.uid}",
    )
    # In the future, we can reduce latency by separating the transfers for requested results
    # from transfers for the full archive. (Use scalems.call._Subprocess.output_filenames.)
    result = RPTaskResult(
        uid=subprocess_task.uid,
        exit_code=subprocess_task.exit_code,
        task_dict=subprocess_task.description,
        directory=sandbox,
        final_state=subprocess_task.state,
        directory_archive=archive_future,
    )
    return result


async def wrapped_function_result_from_rp_task(
    subprocess: scalems.call._Subprocess, rp_task_result: RPTaskResult
) -> scalems.call.CallResult:
    """

    Once `subprocess_to_rp_task` has produced a `RPTaskResult`
    for a `scalems.call._Subprocess`,
    we produce a `scalems.call.CallResult` with the help of some file transfers.

    Args:
        subprocess: the result of a `scalems.call.function_call_to_subprocess`
        rp_task_result: the result of a `subprocess_to_rp_task` for the *subprocess*

    Returns:
        localized result from :py:func:`scalems.call.function_call_to_subprocess()`

    """
    if rp_task_result.final_state != rp.DONE:
        # Note: it is possible that the executable task could fail for reasons other than a Python Exception.
        logger.info(f"Task for {subprocess.uid} in final state {rp_task_result.final_state}.")
        logger.debug(repr(rp_task_result))
    # It looks like this should be encapsulated with the workflow management details
    # of the dispatched work item or further specialized for this specific task type.
    # It doesn't make sense that we should need both the _Subprocess and
    # RPTaskResult objects to be passed by the caller.
    # Also, we don't have access to the requested *outputs* in order to optimize out transfers
    # without adding yet another argument.
    archive_ref = await rp_task_result.directory_archive
    # TODO: Collaborate with scalems.call to agree on output filename.
    output_file = subprocess.output_filenames[0]

    with zipfile.ZipFile(pathlib.Path(archive_ref)) as myzip:
        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug(f"Opening ZipFile {myzip.filename}: {', '.join(myzip.namelist())}")
        with myzip.open(output_file) as fh:
            partial_result: scalems.call.CallResult = scalems.call.deserialize_result(io.TextIOWrapper(fh).read())
    if partial_result.exception is not None:
        assert rp_task_result.final_state != rp.DONE
        logger.error(f"Subprocess {subprocess.uid} encountered Exception: {partial_result.exception}")
    # The remotely packaged result was not aware of the task environment managed by RP.
    # Update and augment the managed file details.
    stdout = copy.deepcopy(rp_task_result.directory).set_path(
        os.path.join(rp_task_result.directory.path, rp_task_result.task_dict["stdout"])
    )
    stderr = copy.deepcopy(rp_task_result.directory).set_path(
        os.path.join(rp_task_result.directory.path, rp_task_result.task_dict["stderr"])
    )

    result = dataclasses.replace(partial_result, stdout=str(stdout), stderr=str(stderr), directory=archive_ref.as_uri())
    return result


async def get_directory_archive(
    directory: radical.saga.Url, dispatcher: RPDispatchingExecutor
) -> scalems.store.FileReference:
    """Get a local archive of a remote directory.

    TODO:
        Let this be absorbed into a collaboration between WorkflowManagement
        contexts and their local and remote data stores.
    """
    # TODO: Let FileStore tell us what directory to use.
    staging_directory = dispatcher.datastore.directory.joinpath(f"_scalems_output_staging_{id(directory)}")
    logger.debug(f"Preparing to stage {directory} to {staging_directory}.")
    assert not staging_directory.exists()

    # TODO: Don't rely on the RP Session. We should be able to do this after the
    #   Session is closed or after an interrupted Session.
    pilot = dispatcher.runtime.pilot()
    # TODO: Can we avoid serializing more than once? Such as with `rp.TARBALL`?
    await asyncio.to_thread(
        pilot.stage_out,
        sds=[
            {
                "source": directory,
                "target": staging_directory.as_uri(),
                "action": rp.TRANSFER,
                "flags": rp.CREATE_PARENTS | rp.RECURSIVE,
            }
        ],
    )
    # Work around https://github.com/radical-cybertools/radical.pilot/issues/2823
    if packaging.version.parse(rp.version) < packaging.version.parse("1.21"):
        while True:
            try:
                staging_directory.stat()
            except FileNotFoundError:
                logger.error(f"Waiting for {staging_directory} to appear...")
                await asyncio.sleep(1.0)
            else:
                break
    try:
        with tempfile.NamedTemporaryFile(mode="wb") as tmp:
            await asyncio.to_thread(
                write_archive, filehandle=tmp, root_dir=staging_directory, relative_to=staging_directory
            )
            tmp.flush()
            archive_path = pathlib.Path(tmp.name).resolve()
            file_ref = await dispatcher.datastore.add_file(scalems.file.describe_file(archive_path))
    finally:
        await asyncio.to_thread(shutil.rmtree, staging_directory)
    return file_ref


def _add_to_archive(archive: zipfile.ZipFile, source: pathlib.Path, relative_to: pathlib.Path):
    destination = source.relative_to(relative_to)
    if source.is_dir():
        for path in source.iterdir():
            _add_to_archive(archive, path, relative_to)
    else:
        if not source.is_file():
            logger.warning(
                "Directory contains unusual filesystem object. "
                f"Attempting to write {source} to {destination} in {archive.filename}"
            )
        archive.write(filename=source, arcname=destination)


def write_archive(*, filehandle, root_dir: pathlib.Path, relative_to: pathlib.Path):
    """Write a ZipFile archive.

    Args:
        filehandle: file-like object to write the archive to
        root_dir: Base of the directory tree to archive
        relative_to: Path prefix to strip from *root_dir* when constructing the paths in the archive.
    """
    assert root_dir.is_dir()
    with zipfile.ZipFile(filehandle, mode="w") as archive:
        _add_to_archive(archive, source=root_dir, relative_to=relative_to)
        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug(f"Wrote to {archive.filename}: {', '.join(archive.namelist())}")


class RuntimeManager:
    """Runtime resources for SCALE-MS workflow execution, backed by RADICAL Pilot.

    Initialize with a Session and pre-configured Pilot job.

    Executors are provisioned in the allocated resources and user configuration
    of a RuntimeManager.

    The RuntimeManager mediates interaction between Executors and WorkflowManager
    to fulfil Futures and update workflow state.

    We expect to have Executor state somewhat disconnected from the calling program
    scope, so the RuntimeManager context is an opportunity to collect Tasks bound
    to different executors, to cleanly shut down executors and release resources.

    The RuntimeManager implements the Computing Provider Interface (CPI) for SCALE-MS
    calls to the runtime provider.`
    """

    runtime: "scalems.radical.runtime.RuntimeSession" = scalems.execution.RuntimeDescriptor()

    def __init__(
        self,
        *,
        datastore: FileStore = None,
        loop: asyncio.AbstractEventLoop,
        configuration: RuntimeConfiguration,
        runtime: scalems.radical.runtime.RuntimeSession,
    ):
        if not isinstance(configuration.target_venv, str) or len(configuration.target_venv) == 0:
            raise ValueError("Caller must specify a venv to be activated by the execution agent for dispatched tasks.")

        self._exception = None
        self._loop: asyncio.AbstractEventLoop = loop

        if datastore is None:
            raise TypeError("Provide a datastore.")
        self.datastore = datastore

        # TODO: Manage the module ContextVars and contextvars.Context scope.
        self._runtime_configuration = configuration

        self.runtime = runtime

    async def wait_closed(self):
        return await self.runtime.wait_closed()

    def close(self):
        """Manage tear down of the RADICAL Pilot Session and resources.

        Several aspects of the RP runtime interface use blocking calls.
        This method should be run in a non-root thread (concurrent.futures.Future)
        that the event loop can manage as an asyncio-wrapped task.

        The method should be used along with the wait_closed() method to allow
        the asyncio event loop to clean up.

        Overrides :py:class:`scalems.execution.RuntimeManager`

        Design note:
            Note that shutdown could take a while. To avoid blocking the event loop,
            we need a consistent strategy for dispatching to threads. We need to
            make sure that we can shut down as cleanly as possible even if the
            asyncio default ThreadPoolExecutor cannot provide an available thread.
            will probably dispatch it to a thread.

            For now, we will try to keep a strategy of performing high level lifetime
            management calls (like `close()`) exclusively from the event loop thread,
            and moving blocking calls to a thread pool at the implementation level.
            If this strategy becomes impractical, we will have to take care when
            mixing `concurrent.futures` logic and `asyncio` logic.

        """
        # WARNING: If this call needs to manipulate ContexVars, we need to be
        #   careful about how it is dispatched by the caller.

        # TODO: Collect or cancel outstanding tasks, shut down executors, and
        #  synchronize the workflow state.

        self.runtime.close()

    async def __aenter__(self):
        if not _shutdown:
            return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: C901
        """Clean up at context exit.

        In addition to handling exceptions, clean up any Session resource.

        We also need to make sure that we properly disengage from any queues
        or generators.

        We can also leave flags for ourself to be checked at __await__,
        if there is a Task associated with the Executor.
        """
        # First, do something with exceptions that occurred in the `with` block if we're going to.
        ...
        # Finally, shut down the runtime.
        try:
            self.close()
            await self.wait_closed()
        except Exception as e:
            logger.exception("Exception during RuntimeManager.close().")
            raise e

        # Only return true if an exception should be suppressed (because it was handled).
        # TODO: Catch internal exceptions for useful logging and user-friendliness.
        if exc_type is not None:
            return False


@contextlib.asynccontextmanager
async def launch(
    workflow_manager: scalems.workflow.WorkflowManager, runtime_configuration: RuntimeConfiguration
) -> RuntimeManager:
    """Get a runtime execution context for the managed workflow.

    Configure, launch, and manage the runtime facilities.
    The resulting RuntimeManager context can be used to acquire an
    Executor for handling task submissions.

    Example::

        with scalems.workflow.scope(workflow, close_on_exit=True):
            async with scalems.radical.runtime.launch(workflow, runtime_config) as runtime_context:
                async with scalems.execution.executor(runtime_context, requirements={}) as executor:
                    task: concurrent.futures.Future = executor.submit(fn, *args, **kwargs)
                    task: asyncio.Task = asyncio.create_task(loop.run_in_executor(executor, fn, *args))

    """
    if workflow_manager.closed:
        raise scalems.exceptions.ScopeError("WorkflowManager is closed.")

    if "RADICAL_PILOT_DBURL" not in os.environ:
        raise DispatchError("RADICAL Pilot environment is not available.")

    if not isinstance(runtime_configuration.target_venv, str) or len(runtime_configuration.target_venv) == 0:
        raise ValueError("Caller must specify a venv to be activated by the execution agent for dispatched tasks.")

    with _global_shutdown_lock:
        if _shutdown:
            return
        # TODO: Check that we have a FileStore.

        logger.debug("Entering RP dispatching context. Waiting for rp.Session.")

        # Note that any thread-dispatched creation functions or other subtasks
        # in `runtime_session` will see the contextvars.Context state from _before_
        # RuntimeManager takes control of its scope. Be careful to keep in mind
        # appropriate coupling and collective state transitions of RuntimeSession
        # and RuntimeManager.
        _runtime: RuntimeSession = await runtime_session(
            configuration=runtime_configuration, loop=workflow_manager.loop()
        )

        # TODO: Asynchronous data staging optimization.
        #  Allow input data staging to begin before scheduler is in state EXECUTING and
        #  before Pilot is in state PMGR_ACTIVE.

        # Raptor session will be initialized and attached in the executor context, if needed.
        assert _runtime.raptor is None

        runtime_context = RuntimeManager(
            datastore=workflow_manager.datastore(),
            loop=workflow_manager.loop(),
            configuration=runtime_configuration,
            runtime=_runtime,
        )

    try:
        # Manage scope of executor operation with a context manager.
        # RP does not yet use an event loop, but we can use async context manager
        # for future compatibility with asyncio management of network connections,
        # etc.
        #
        # Note: the executor owns a rp.Session during operation.
        yield runtime_context

    except Exception as e:
        logger.exception("Unhandled exception while in dispatching context.")
        raise e

    finally:
        close_task = asyncio.create_task(asyncio.to_thread(runtime_context.close), name="runtime_manager.close")
        await close_task
        logger.debug(f"Exited {runtime_context} context.")
