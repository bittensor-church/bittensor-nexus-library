from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, NewType, override

from nexus.actors import Timestamped
from nexus.core.runtime.context_store import Context
from nexus.utils.types import Epoch

NexusTaskName = NewType("NexusTaskName", str)


class TaskResultStore(ABC):
    @abstractmethod
    def add_task_result[Result](self, ctx: Context, task_name: NexusTaskName, result: Timestamped[Result]) -> None:
        """
        Appends a new task result to the store,
        makes a relevant log entry in the Context
        """
        pass

    @abstractmethod
    def get_tasks_for_epoch(self, task_name: NexusTaskName, epoch: Epoch) -> Sequence[Timestamped[Any]]:
        """
        Retrieves all task results for a given task name and epoch.
         - The results should be returned in chronological order by block number (oldest first).
         - The task result epoch is determined based on the block in the timestamp of the task results
         - The implementation should be efficient in retrieving results for a specific epoch,
            even if the store contains a large number of results.
        """
        pass


class InMemoryTaskResultStore(TaskResultStore):
    def __init__(self):
        self.store: dict[NexusTaskName, list[Timestamped[Any]]] = {}

    @override
    def add_task_result[Result](self, ctx: Context, task_name: NexusTaskName, result: Timestamped[Result]) -> None:
        if task_name not in self.store:
            self.store[task_name] = []
        self.store[task_name].append(result)
        ctx.append_user_note(f"Added task result for {task_name} at block {result.block_at_finish}")

    @override
    def get_tasks_for_epoch(self, task_name: NexusTaskName, epoch: Epoch) -> Sequence[Timestamped[Any]]:
        if task_name not in self.store:
            return []
        results = [result for result in self.store[task_name] if epoch.contains(result.block_at_finish.block_number)]
        results.sort(key=lambda r: r.block_at_finish.block_number)
        return results
