import threading
import uuid
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import override

from pylon_client.artanis.v1 import Neuron

from nexus.actors.chain_beat.block_beat import BlockBeat
from nexus.actors.executor_communicator import ProcessedInput
from nexus.actors.neuron_router import Routed
from nexus.actors.timestamper import Timestamped
from nexus.core.runtime.context_store import Context
from nexus.core.runtime.nexus_task_types import NexusTaskName, TaskResultId
from nexus.utils.exceptions import ExecutorFailureException, NexusException, TaskResultNotFoundException
from nexus.utils.types import Epoch, Hotkey

type StoredTaskExecution[ExecutorPayload, Output] = Timestamped[ProcessedInput[Routed[ExecutorPayload], Output]]


@dataclass(frozen=True)
class TaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
    result: StoredTaskExecution[ExecutorPayload, ExecutorOutput]
    executor_public_output: ExecutorPublicOutput | None


@dataclass(frozen=True)
class SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
    id: TaskResultId
    result: StoredTaskExecution[ExecutorPayload, ExecutorOutput]
    executor_public_output: ExecutorPublicOutput | None

    @property
    def processing_started(self) -> datetime:
        return self.result.processing_started

    @property
    def processing_finished(self) -> datetime:
        return self.result.processing_finished

    @property
    def block_at_finish(self) -> BlockBeat:
        return self.result.block_at_finish

    @property
    def executor_payload(self) -> ExecutorPayload:
        return self.result.executor_output.input.input

    @property
    def executor_output(self) -> ExecutorOutput | NexusException:
        return self.result.executor_output.output

    @property
    def is_failure(self) -> bool:
        return isinstance(self.result.executor_output.output, NexusException)

    @property
    def target(self) -> Neuron:
        return self.result.executor_output.input.target


class TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](ABC):
    """Interface for storing and querying NexusTask results.

    Implementations must be thread-safe.
    """

    @abstractmethod
    def add_task_result(
        self,
        ctx: Context,
        task_name: NexusTaskName,
        result: TaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ) -> SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        """
        Appends a new task result to the store, makes a relevant log entry in the Context,
        and returns the stored result.
        """
        pass

    @abstractmethod
    def get_task_result(
        self,
        task_name: NexusTaskName,
        task_result_id: TaskResultId,
    ) -> SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        """Retrieves one task result for a given task name and task-result id.

        Raises:
            TaskResultNotFoundException: If no result exists for the given task name and result id.
        """
        pass

    @abstractmethod
    def get_tasks_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
    ) -> tuple[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...]:
        """
        Retrieves all task results for a given task name and epoch.
         - The results should be returned in chronological order by block number (oldest first).
         - The task result epoch is determined based on the block in the timestamp of the task results
         - The implementation should be efficient in retrieving results for a specific epoch,
            even if the store contains a large number of results.
        """
        pass

    def count_by_hotkey_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
        *,
        include_executor_failures: bool = True,
    ) -> Mapping[Hotkey, int]:
        """
        Counts task results for a given task and epoch grouped by routed neuron hotkey.
        """
        results = self.get_tasks_for_epoch(task_name=task_name, epoch=epoch)
        if include_executor_failures:
            return Counter(Hotkey(result.target.hotkey) for result in results)
        else:
            return Counter(
                Hotkey(result.target.hotkey)
                for result in results
                if not isinstance(result.executor_output, ExecutorFailureException)
            )


class InMemoryTaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
):
    store: dict[NexusTaskName, list[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]]
    by_id: dict[
        NexusTaskName,
        dict[TaskResultId, SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]],
    ]
    lock: threading.Lock

    def __init__(self) -> None:
        self.store = {}
        self.by_id = {}
        self.lock = threading.Lock()

    @override
    def add_task_result(
        self,
        ctx: Context,
        task_name: NexusTaskName,
        result: TaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ) -> SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        entry = SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
            id=TaskResultId(uuid.uuid7()),
            result=result.result,
            executor_public_output=result.executor_public_output,
        )
        with self.lock:
            if task_name not in self.store:
                self.store[task_name] = []
                self.by_id[task_name] = {}
            self.store[task_name].append(entry)
            self.by_id[task_name][entry.id] = entry
            ctx.append_user_note(
                f"Added task result for {task_name} at block {result.result.block_at_finish.block_number}"
            )
            return entry

    @override
    def get_task_result(
        self,
        task_name: NexusTaskName,
        task_result_id: TaskResultId,
    ) -> SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        with self.lock:
            task_results = self.by_id.get(task_name)
            if task_results is None or task_result_id not in task_results:
                raise TaskResultNotFoundException(task_name=task_name, task_result_id=task_result_id)
            return task_results[task_result_id]

    @override
    def get_tasks_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
    ) -> tuple[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...]:
        with self.lock:
            if task_name not in self.store:
                return ()
            results = [
                result for result in self.store[task_name] if epoch.contains(result.result.block_at_finish.block_number)
            ]
            results.sort(key=lambda r: r.result.block_at_finish.block_number)
            return tuple(results)
