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
from nexus.utils.exceptions import (
    ExecutorFailureException,
    InternalFrameworkException,
    NexusException,
    TaskResultNotFoundException,
)
from nexus.utils.types import Epoch, Hotkey

type StoredTaskExecution[ExecutorPayload, Output] = Timestamped[ProcessedInput[Routed[ExecutorPayload], Output]]


@dataclass(frozen=True)
class SuccessfulTaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
    """Persist payload for one successful executor attempt."""

    result: StoredTaskExecution[ExecutorPayload, ExecutorOutput]
    executor_public_output: ExecutorPublicOutput


@dataclass(frozen=True)
class ExecutorFailureTaskResultToPersist[ExecutorPayload]:
    """Persist payload for one executor failure attempt."""

    result: StoredTaskExecution[ExecutorPayload, ExecutorFailureException]


@dataclass(frozen=True)
class TaskResultBase[ExecutorPayload]:
    """Flat persisted task-result metadata shared by all record kinds."""

    id: TaskResultId
    processing_started: datetime
    processing_finished: datetime
    block_at_finish: BlockBeat
    executor_payload: ExecutorPayload
    target: Neuron


@dataclass(frozen=True)
class SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](TaskResultBase[ExecutorPayload]):
    """Persisted task result for one successful executor attempt."""

    executor_output: ExecutorOutput
    executor_public_output: ExecutorPublicOutput


@dataclass(frozen=True)
class ExecutorFailureTaskResult[ExecutorPayload](TaskResultBase[ExecutorPayload]):
    """Persisted task result for one executor failure attempt."""

    executor_failure: ExecutorFailureException


class TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](ABC):
    """Interface for storing and querying split Nexus task results.

    Implementations must be thread-safe.
    """

    @abstractmethod
    def add_successful_task_result(
        self,
        ctx: Context,
        task_name: NexusTaskName,
        result: SuccessfulTaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ) -> SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        """
        Appends a successful task result to the store, writes a context log entry,
        and returns the stored record.
        """
        pass

    @abstractmethod
    def add_executor_failure(
        self,
        ctx: Context,
        task_name: NexusTaskName,
        result: ExecutorFailureTaskResultToPersist[ExecutorPayload],
    ) -> ExecutorFailureTaskResult[ExecutorPayload]:
        """
        Appends an executor failure task result to the store, writes a context log entry,
        and returns the stored record.
        """
        pass

    @abstractmethod
    def get_task_result(
        self,
        task_name: NexusTaskName,
        task_result_id: TaskResultId,
    ) -> (
        SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
        | ExecutorFailureTaskResult[ExecutorPayload]
    ):
        """Retrieves one task result for a given task name and task-result id.

        Raises:
            TaskResultNotFoundException: If no result exists for the given task name and result id.
        """
        pass

    @abstractmethod
    def get_successful_tasks_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
    ) -> tuple[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...]:
        """
        Retrieves all successful task results for a given task name and epoch.
         - The results should be returned in chronological order by block number (oldest first).
         - The task result epoch is determined based on the block in the timestamp of the task results.
         - The implementation should be efficient in retrieving results for a specific epoch,
            even if the store contains a large number of results.
        """
        pass

    @abstractmethod
    def get_executor_failures_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
    ) -> tuple[ExecutorFailureTaskResult[ExecutorPayload], ...]:
        """
        Retrieves all executor failure task results for a given task name and epoch.
         - The results should be returned in chronological order by block number (oldest first).
         - The task result epoch is determined based on the block in the timestamp of the task results.
         - The implementation should be efficient in retrieving results for a specific epoch,
            even if the store contains a large number of results.
        """
        pass

    def count_successful_by_hotkey_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
    ) -> Mapping[Hotkey, int]:
        """Count successful task results for a given task and epoch grouped by routed neuron hotkey."""

        return Counter(Hotkey(result.target.hotkey) for result in self.get_successful_tasks_for_epoch(task_name, epoch))

    def count_executor_failures_by_hotkey_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
    ) -> Mapping[Hotkey, int]:
        """Count executor failure task results for a given task and epoch grouped by routed neuron hotkey."""

        return Counter(
            Hotkey(result.target.hotkey) for result in self.get_executor_failures_for_epoch(task_name, epoch)
        )


class InMemoryTaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
):
    """Thread-safe in-memory split task-result store for tests and local use."""

    successful_store: dict[
        NexusTaskName,
        list[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]],
    ]
    executor_failure_store: dict[NexusTaskName, list[ExecutorFailureTaskResult[ExecutorPayload]]]
    by_id: dict[
        NexusTaskName,
        dict[
            TaskResultId,
            SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
            | ExecutorFailureTaskResult[ExecutorPayload],
        ],
    ]
    lock: threading.Lock

    def __init__(self) -> None:
        self.successful_store = {}
        self.executor_failure_store = {}
        self.by_id = {}
        self.lock = threading.Lock()

    @override
    def add_successful_task_result(
        self,
        ctx: Context,
        task_name: NexusTaskName,
        result: SuccessfulTaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ) -> SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        executor_output = result.result.executor_output.output
        if isinstance(executor_output, NexusException):
            raise InternalFrameworkException(
                f"Expected successful executor output for task {task_name}, got {type(executor_output).__name__}"
            )
        if result.executor_public_output is None:
            raise InternalFrameworkException(
                f"Successful task result for task {task_name} requires executor_public_output"
            )

        entry = SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
            id=TaskResultId(uuid.uuid7()),
            processing_started=result.result.processing_started,
            processing_finished=result.result.processing_finished,
            block_at_finish=result.result.block_at_finish,
            executor_payload=result.result.executor_output.input.input,
            target=result.result.executor_output.input.target,
            executor_output=executor_output,
            executor_public_output=result.executor_public_output,
        )
        with self.lock:
            if task_name not in self.successful_store:
                self.successful_store[task_name] = []
                self.executor_failure_store[task_name] = []
                self.by_id[task_name] = {}
            self.successful_store[task_name].append(entry)
            self.by_id[task_name][entry.id] = entry
            ctx.append_user_note(
                f"Added successful task result for {task_name} at block {result.result.block_at_finish.block_number}"
            )
            return entry

    @override
    def add_executor_failure(
        self,
        ctx: Context,
        task_name: NexusTaskName,
        result: ExecutorFailureTaskResultToPersist[ExecutorPayload],
    ) -> ExecutorFailureTaskResult[ExecutorPayload]:
        executor_failure = result.result.executor_output.output
        if not isinstance(executor_failure, ExecutorFailureException):
            raise InternalFrameworkException(
                f"Expected executor failure output for task {task_name}, got {type(executor_failure).__name__}"
            )

        entry = ExecutorFailureTaskResult[ExecutorPayload](
            id=TaskResultId(uuid.uuid7()),
            processing_started=result.result.processing_started,
            processing_finished=result.result.processing_finished,
            block_at_finish=result.result.block_at_finish,
            executor_payload=result.result.executor_output.input.input,
            target=result.result.executor_output.input.target,
            executor_failure=executor_failure,
        )
        with self.lock:
            if task_name not in self.executor_failure_store:
                self.successful_store[task_name] = []
                self.executor_failure_store[task_name] = []
                self.by_id[task_name] = {}
            self.executor_failure_store[task_name].append(entry)
            self.by_id[task_name][entry.id] = entry
            ctx.append_user_note(
                "Added executor failure task result for "
                f"{task_name} at block {result.result.block_at_finish.block_number}"
            )
            return entry

    @override
    def get_task_result(
        self,
        task_name: NexusTaskName,
        task_result_id: TaskResultId,
    ) -> (
        SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
        | ExecutorFailureTaskResult[ExecutorPayload]
    ):
        with self.lock:
            task_results = self.by_id.get(task_name)
            if task_results is None or task_result_id not in task_results:
                raise TaskResultNotFoundException(task_name=task_name, task_result_id=task_result_id)
            return task_results[task_result_id]

    @override
    def get_successful_tasks_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
    ) -> tuple[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...]:
        with self.lock:
            if task_name not in self.successful_store:
                return ()
            results = [
                result
                for result in self.successful_store[task_name]
                if epoch.contains(result.block_at_finish.block_number)
            ]
            results.sort(key=lambda r: r.block_at_finish.block_number)
            return tuple(results)

    @override
    def get_executor_failures_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
    ) -> tuple[ExecutorFailureTaskResult[ExecutorPayload], ...]:
        with self.lock:
            if task_name not in self.executor_failure_store:
                return ()
            results = [
                result
                for result in self.executor_failure_store[task_name]
                if epoch.contains(result.block_at_finish.block_number)
            ]
            results.sort(key=lambda r: r.block_at_finish.block_number)
            return tuple(results)
