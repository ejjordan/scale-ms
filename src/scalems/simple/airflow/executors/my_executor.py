from typing import List

from airflow.executors.base_executor import BaseExecutor
from airflow.utils.state import TaskInstanceState
from airflow.models.taskinstancekey import TaskInstanceKey
import subprocess

class MyExecutor(BaseExecutor):
    def __init__(self):
        super().__init__()
        self.commands_to_run = []

    def execute_async(
        self,
            key: TaskInstanceKey,
            command: List[str],
            queue = None,
            executor_config = None,
    ) -> None:
        self.validate_airflow_tasks_run_command(command)
        self.commands_to_run.append((key, command))

    def sync(self) -> None:
        for key, command in self.commands_to_run:
            self.log.info("Executing command: %s", command)

            try:
                subprocess.check_call(command, close_fds=True)
                self.change_state(key, TaskInstanceState.SUCCESS)
            except subprocess.CalledProcessError as e:
                self.change_state(key, TaskInstanceState.FAILED)
                self.log.error("Failed to execute task %s.", e)

        self.commands_to_run = []

    def end(self):
        """End the executor."""
        self.heartbeat()
