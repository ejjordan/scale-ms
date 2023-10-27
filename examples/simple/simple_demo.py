"""
Usage:
python simple_demo.py --venv path/to/scalems/venv/ --resource local.localhost
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import os
import sys
from dataclasses import dataclass

from pathlib import Path

import scalems.radical
import radical.pilot as rp

from scalems.radical.raptor import RaptorConfiguration, ScaleMSRaptor, ClientWorkerRequirements


#import scalems.simple as simple
#from scalems.store import FileStore, FileStoreManager

def get_pilot_desc(resource: str = 'local.localhost'):
    description = rp.PilotDescription({'resource': resource,
                            'runtime'      : 30,  # pilot runtime minutes
               'exit_on_error': False,
               'project'      : None,
               'queue'        : None,
               'cores'        : 4,
               'gpus'         : 0,})
    return description

@rp.pythontask
def run_gmxapi_radical(args, input_files, output_files):
    import gmxapi as gmx
    cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), args, input_files, output_files)
    return cmd.output.file.result()

@dataclass
class WorkItem:
    args: list
    """Positional arguments for *func*."""

    kwargs: dict
    """Key word arguments for *func*."""

    func: str = run_gmxapi_radical.__name__
    """A callable to be retrieved as an attribute in *module*."""

    module: str = run_gmxapi_radical.__module__
    """The qualified name of a module importable by the Worker."""

def raptor_task_from_work_item(work_item: WorkItem, env_name: str = None):
    """Create a Raptor task from a WorkItem.

    :param work_item: A WorkItem object.
    :param env_name: Name of the environment to use for the task.
    :return: A Raptor task.
    """
    task = {
        "mode": rp.TASK_FUNCTION,
        "func": work_item.func,
        "args": work_item.args,
        "kwargs": work_item.kwargs,
        "env_name": env_name,
    }
    return task


class GmxApiRun:
    """Instance of a simulation Command."""

    def __init__(
        self,
        command_line_args, input_files, output_files,
        venv,
        label: str,
        datastore: scalems.store.FileStore,
    ):

        # TODO: Manage input file staging so we don't have to assume localhost.
        args = (command_line_args, input_files, output_files,)
        self.label = label
        self._call_handle = scalems.call.function_call_to_subprocess(
                func=self._func,
                label=label,
                args=args,
                kwargs=None,
                datastore=datastore,
                venv=venv,
            )

    @staticmethod
    def _func(*args):
        """Task implementation."""
        import gmxapi as gmx
        cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), *args)
        return cmd.output.file.result()

    async def result(self, dispatcher: scalems.radical.runtime.RPDispatchingExecutor):
        """Deliver the results of the simulation Command."""
        # Wait for input preparation
        #import ipdb;ipdb.set_trace()
        call_handle = await asyncio.create_task(self._call_handle)
        rp_task_result_future = asyncio.create_task(
            scalems.radical.task.subprocess_to_rp_task(call_handle, dispatcher=dispatcher)
        )
        # Wait for submission and completion
        rp_task_result = await rp_task_result_future
        result_future = asyncio.create_task(
            scalems.radical.task.wrapped_function_result_from_rp_task(call_handle, rp_task_result)
        )
        # Wait for results staging.
        result: scalems.call.CallResult = await result_future
        # Note that the return_value is the trajectory path in the RP-managed Task directory.
        # TODO: stage trajectory file, explicitly?
        return {"trajectory": result.return_value, "directory": result.directory}

class GmxRun:
    """Instance of a simulation Command."""

    def __init__(
        self,
        command_line_args, input_files, output_files,
        label: str,
        datastore: scalems.store.FileStore,
        dispatcher: scalems.radical.runtime.RPDispatchingExecutor,
    ):
        self.label = label

        # TODO: Manage input file staging so we don't have to assume localhost.
        args = (command_line_args, input_files, output_files,)
        task_uid = label
        self._call_handle: asyncio.Task[scalems.call._Subprocess] = asyncio.create_task(
            scalems.call.function_call_to_subprocess(
                func=self._func,
                label=task_uid,
                args=args,
                kwargs=None,
                datastore=datastore,
            )
        )
        self._dispatcher = dispatcher

    @staticmethod
    def _func(*args):
        """Task implementation."""
        import gmxapi as gmx
        cmd = gmx.commandline_operation(gmx.commandline.cli_executable(), *args)
        return cmd.output.file.result()

    async def result(self):
        """Deliver the results of the simulation Command."""
        # Wait for input preparation
        call_handle = await self._call_handle
        rp_task_result_future = asyncio.create_task(
            scalems.radical.task.subprocess_to_rp_task(call_handle, dispatcher=self._dispatcher)
        )
        # Wait for submission and completion
        rp_task_result = await rp_task_result_future
        result_future = asyncio.create_task(
            scalems.radical.task.wrapped_function_result_from_rp_task(call_handle, rp_task_result)
        )
        # Wait for results staging.
        result: scalems.call.CallResult = await result_future
        # Note that the return_value is the trajectory path in the RP-managed Task directory.
        # TODO: stage trajectory file, explicitly?
        return {"trajectory": result.return_value, "directory": result.directory}

async def launch(dispatcher, simulations):
    futures = tuple(asyncio.create_task(md.result(dispatcher), name=md.label) for md in simulations)
    #import ipdb;ipdb.set_trace()
    for future in futures:
        future.add_done_callback(lambda x: print(f"Task done: {repr(x)}."))
    return await asyncio.gather(*futures)

async def launch_mdrun(command_line_args, input_files, output_files,
        label: str,
        datastore: scalems.store.FileStore,
        dispatcher: scalems.radical.runtime.RPDispatchingExecutor):

        run_gmx = GmxRun(
                command_line_args, input_files, output_files,
                datastore=datastore,
                dispatcher=dispatcher,
                label=f"{label}",
            )
        future = asyncio.create_task(run_gmx.result())
        future.add_done_callback(lambda x: print(f"Task done: {repr(x)}."))
        return await asyncio.gather(*future)

def sample_callable(prefix: str, *args):
    """Compose a string."""
    result = f"{prefix}: " + ", ".join(str(item) for item in args)
    return result

if __name__ == "__main__":
    import gmxapi as gmx

    # Set up a command line argument processor for our script.
    # Inherit from the backend parser so that `parse_known_args` can handle positional arguments the way we want.
    parser = argparse.ArgumentParser(
        parents=[scalems.radical.runtime_configuration.parser()],
        add_help=True,
        description="Parser for simple demo script.",
    )
    parser.add_argument(
        "--input_dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent.parent / "testdata" / "alanine-dipeptide",
        help="Directory containing alanine dipeptide input files. (default: %(default)s)",
    )
    parser.add_argument(
        "--input_gro",
        type=Path,
        default="equil3.gro",
        help="Name of initial gro file to use. (default: %(default)s)",
    )
    # This is necessary to make the script not hang here (???)
    sys.argv.insert(0, __file__)

    script_config, argv = parser.parse_known_args()
    runtime_configuration = scalems.radical.runtime_configuration.configuration(argv)

    input_dir = script_config.input_dir
    input_top = os.path.join(input_dir, "topol.top")
    input_mdp = os.path.join(input_dir, "grompp.mdp")
    input_gro = os.path.join(input_dir, 'equil3.gro')
    input_files={'-f': input_mdp, '-p': input_top, '-c': input_gro,}
    results_dir = os.path.join(os.getcwd(), 'results_dir')

    work_item = WorkItem(args=['grompp', input_files, {'-o': 'run.tpr'}], kwargs={})
    task_list = raptor_task_from_work_item(work_item, 'local')

    pilot_description = get_pilot_desc(script_config.resource)

    import packaging.version
    client_scalems_version = packaging.version.Version(scalems.__version__)
    if client_scalems_version.is_prerelease:
        minimum_scalems_version = client_scalems_version.public
    else:
        minimum_scalems_version = client_scalems_version.base_version
    #versioned_modules = (("scalems", minimum_scalems_version), ("radical.pilot", rp.version_short))
    #configuration = RaptorConfiguration(versioned_modules=list(versioned_modules),)

    #conf_dict: scalems.radical.raptor._RaptorConfigurationDict = dataclasses.asdict(configuration)
    #configuration = RaptorConfiguration.from_dict(conf_dict)
    #master = ScaleMSRaptor(configuration)

    num_workers = 1
    worker_processes = 1
    gpus_per_process = 0

    worker_requirements = [ClientWorkerRequirements(
        named_env="scalems_test_ve", cpu_processes=worker_processes, cores_per_process=1, gpus_per_rank=gpus_per_process
    )]*2
    #worker_config = master.configure_worker(worker_requirements)

    runtime_config = scalems.radical.runtime_configuration.configuration(
        execution_target=pilot_description.resource,
        target_venv=script_config.venv,
        rp_resource_params={"PilotDescription": pilot_description.as_dict()},
        enable_raptor=False,
    )
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    workflow = scalems.radical.workflow_manager(loop)
    #scope = scalems.workflow.scope(workflow, close_on_exit=True)
    #dispatcher=scalems.execution.dispatch(workflow, executor_factory=scalems.radical.executor_factory, params=runtime_config)

    executor=scalems.radical.runtime.executor_factory(workflow, runtime_config)
    executor.rt_startup()
    async def startup(executor):
        runner_task: asyncio.Task = executor.rt_startup()
        await runner_task



    #asyncio.run(startup(executor))
    #import ipdb;ipdb.set_trace()
    #session_cfg = rp.utils.get_resource_config(resource='local.localhost')
    #session = rp.Session(cfg=session_cfg)
    #pmgr=rp.PilotManager(session)
    #tmgr=rp.TaskManager(session)

    task_description_dict = dict(
        executable="/bin/bash",
        arguments=["-c", "/bin/sleep 5 && /bin/echo success"],
        cpu_processes=1,
    )
    task_description = rp.TaskDescription(from_dict=task_description_dict)
    task_description.uid = "test-rp-future-rp-cancellation-propagation"

    async def sub(dispatcher, workflow):
        from subprocess import run as subprocess_run
        call_handle: scalems.call._Subprocess = await scalems.call.function_call_to_subprocess(
        func=subprocess_run,
        kwargs={"args": ["/bin/echo", "hello", "world"], "capture_output": True},
        label="test_rp_function-1",
        manager=workflow,
        requirements=None,)
        rp_task_result: scalems.radical.task.RPTaskResult = await scalems.radical.task.subprocess_to_rp_task(
                call_handle, dispatcher=dispatcher
            )
        call_result: scalems.call.CallResult = await scalems.radical.task.wrapped_function_result_from_rp_task(
                call_handle, rp_task_result
            )
        return call_result



    work_item = scalems.radical.raptor.ScalemsRaptorWorkItem(
        func=print.__name__, module=print.__module__, args=["hello world"], kwargs={}, comm_arg_name=None
    )
    #cpi_call = scalems.cpi.add_item(work_item)

    #cmd2 = scalems.executable(("/bin/echo", "hello", "world"), stdout="stdout2.txt")

    import tempfile, pathlib
    temp_dir=tempfile.TemporaryDirectory()
    temp_dir_path=pathlib.Path(temp_dir.name)
    wfm=scalems.workflow.WorkflowManager(loop=loop, directory=temp_dir.name)
    #qqq=scalems.execution.Queuer(source=wfm, command_queue=asyncio.Queue)

    #input=scalems.subprocess.SubprocessInput(["/bin/echo", "hello", "world"], stdout="stdout2.txt")
    #director = scalems.workflow.workflow_item_director_factory(scalems.subprocess.Subprocess, manager=wfm)
    #task_view=director(input=input)

    gar = GmxApiRun(command_line_args='grompp',
                input_files=input_files,
                output_files={'-o': "run.tpr"},
                label=f'run-{0}',
                datastore=wfm.datastore(),
                venv=script_config.venv,
               )
    result=asyncio.run(launch(executor, [gar]))
    #asyncio.run(launch_mdrun(command_line_args='grompp',input_files=input_files,output_files={'-o': "run.tpr"},label=f'run-{0}',datastore=wfm.datastore(),dispatcher=executor))
    import ipdb;ipdb.set_trace()
    """
    work_item0 = simple.WorkItem(
        args=['grompp', input_files, {'-o': 'run.tpr'}], kwargs={}, round_name='round1', step_number=0, tasks_in_step=1)

    manager = FileStoreManager()
    datastore: scalems.store.FileStore = manager.filestore()
    task_list0 = simple.raptor_task_from_work_item(work_item0, 'local')

    #import ipdb;ipdb.set_trace()
    desc = simple.get_pilot_desc(script_config.resource)

    tpr0 = os.path.join('scalems_0_0',f'{task_list0[0].uid}.run.tpr')
    print(f"tpr path: {tpr0}")

    work_item1 = simple.WorkItem(
        args=[['mdrun', '-ntomp', '2'], {'-s': tpr0}, {'-x':'result.xtc', '-c': 'result.gro'}],
        kwargs={}, round_name='round1', step_number=1, tasks_in_step=1)
    task_list1 = simple.raptor_task_from_work_item(work_item1, 'local')
    task_list1[0]['output_staging'] = list()
    import radical.pilot as rp
    task_list1[0]['input_staging'] = {'source': f'client:///{tpr0}',
                                      'target': f'task:///{tpr0}',
                                      'action': rp.TRANSFER}
    #import ipdb;ipdb.set_trace()

    sm = simple.SimpleManager(True)
    sm.prepare_raptor(desc, script_config.venv)
    outs0 = asyncio.run(sm.run_queue(task_list0))
    sm.wait_tasks(outs0)
    outs1 = asyncio.run(sm.run_queue(task_list1))
    sm.wait_tasks(outs1)
    #import ipdb;ipdb.set_trace()

    sm.close()
    """