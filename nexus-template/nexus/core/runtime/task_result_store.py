import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import override

from nexus.actors import Timestamped
from nexus.core.runtime.context_store import Context
from nexus.core.runtime.nexus_task_types import NexusTaskName, TaskResultId
from nexus.utils.types import Epoch


@dataclass(frozen=True)
class SingleTaskResult[Result]:
    id: TaskResultId
    result: Timestamped[Result]


class TaskResultStore[Result](ABC):
    """ "Interface for storing and retrieving task results.

    implementations must be thread-safe
    """

    @abstractmethod
    def add_task_result(self, ctx: Context, task_name: NexusTaskName, result: Timestamped[Result]) -> TaskResultId:
        """
        Appends a new task result to the store,
        makes a relevant log entry in the Context
        """
        pass

    @abstractmethod
    def get_tasks_for_epoch(self, task_name: NexusTaskName, epoch: Epoch) -> tuple[SingleTaskResult[Result], ...]:
        """
        Retrieves all task results for a given task name and epoch.
         - The results should be returned in chronological order by block number (oldest first).
         - The task result epoch is determined based on the block in the timestamp of the task results
         - The implementation should be efficient in retrieving results for a specific epoch,
            even if the store contains a large number of results.
        """
        pass


class InMemoryTaskResultStore[Result](TaskResultStore[Result]):
    store: dict[NexusTaskName, list[SingleTaskResult[Result]]]
    lock: threading.Lock

    def __init__(self):
        self.store = {}
        self.lock = threading.Lock()

    @override
    def add_task_result(self, ctx: Context, task_name: NexusTaskName, result: Timestamped[Result]) -> TaskResultId:
        entry = SingleTaskResult(id=TaskResultId(uuid.uuid7()), result=result)
        with self.lock:
            if task_name not in self.store:
                self.store[task_name] = []
            self.store[task_name].append(entry)
            ctx.append_user_note(f"Added task result for {task_name} at block {result.block_at_finish}")
            return entry.id

    @override
    def get_tasks_for_epoch(self, task_name: NexusTaskName, epoch: Epoch) -> tuple[SingleTaskResult[Result], ...]:
        with self.lock:
            if task_name not in self.store:
                return ()
            results = [
                result for result in self.store[task_name] if epoch.contains(result.result.block_at_finish.block_number)
            ]
            results.sort(key=lambda r: r.result.block_at_finish.block_number)
            return tuple(results)
