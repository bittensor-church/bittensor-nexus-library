# pyright: basic

from typing import cast, override

from transform_test_utils import TransformActorTestSetupFactory
from utils import (
    DEFAULT_TEST_NETUID,
    build_nexus_task_result,
    empty_context_store,
    get_epoch_containing_block,
    wait_until,
)

from nexus.v1 import (
    BlockNumber,
    Context,
    ExecutorFailureException,
    ExecutorFailureTaskResult,
    ExecutorFailureTaskResultStorer,
    ExecutorFailureTaskResultToPersist,
    InMemoryTaskResultStore,
    NexusException,
    NexusTaskName,
    NodeId,
    StoredTaskExecution,
    SuccessfulTaskResult,
    SuccessfulTaskResultToPersist,
    TaskResultStore,
    TaskResultStoreProvider,
)

type DummyExecutorPayload = str
type DummyExecutorOutput = int
type DummyExecutorPublicOutput = str


class ExecutorFailureStoreWriteException(NexusException):
    pass


class ExplodingExecutorFailureTaskResultStore(
    InMemoryTaskResultStore[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput]
):
    failure: NexusException

    def __init__(self, failure: NexusException) -> None:
        super().__init__()
        self.failure = failure

    @override
    def add_executor_failure(
        self,
        ctx: Context,
        task_name: NexusTaskName,
        result: ExecutorFailureTaskResultToPersist[DummyExecutorPayload],
    ) -> ExecutorFailureTaskResult[DummyExecutorPayload]:
        raise self.failure


class TaskResultStoreProviderDouble(
    TaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput]
):
    _store: TaskResultStore[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput]

    def __init__(
        self,
        store: TaskResultStore[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput],
    ) -> None:
        self._store = store

    @override
    def get_task_result_store(
        self,
    ) -> TaskResultStore[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput]:
        return self._store


def test_in_memory_task_result_store_separates_successes_from_executor_failures() -> None:
    task_name = NexusTaskName("task-result-store-split")
    block_number = 123
    epoch = get_epoch_containing_block(BlockNumber(block_number), netuid=DEFAULT_TEST_NETUID)
    store = InMemoryTaskResultStore[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput]()

    successful_execution = build_nexus_task_result(
        executor_payload="input-1",
        output=7,
        block_number=block_number,
        target_hotkey="task-result-storer-neuron",
    )
    executor_failure_execution = cast(
        StoredTaskExecution[DummyExecutorPayload, ExecutorFailureException],
        build_nexus_task_result(
            executor_payload="input-2",
            output=cast(
                DummyExecutorOutput | NexusException,
                ExecutorFailureException(NexusException("executor boom")),
            ),
            block_number=block_number,
            target_hotkey="task-result-storer-neuron",
        ),
    )

    with empty_context_store().create_context() as successful_ctx:
        successful_result = store.add_successful_task_result(
            successful_ctx,
            task_name,
            SuccessfulTaskResultToPersist(
                result=successful_execution,
                executor_public_output="public-output-1",
            ),
        )
    with empty_context_store().create_context() as failure_ctx:
        executor_failure_result = store.add_executor_failure(
            failure_ctx,
            task_name,
            ExecutorFailureTaskResultToPersist(result=executor_failure_execution),
        )

    assert isinstance(successful_result, SuccessfulTaskResult)
    assert isinstance(executor_failure_result, ExecutorFailureTaskResult)
    assert store.get_successful_tasks_for_epoch(task_name, epoch) == (successful_result,)
    assert store.get_executor_failures_for_epoch(task_name, epoch) == (executor_failure_result,)
    assert store.get_successful_tasks_for_epoch(task_name, epoch) != store.get_executor_failures_for_epoch(
        task_name, epoch
    )


def test_in_memory_task_result_store_get_task_result_returns_both_result_kinds() -> None:
    task_name = NexusTaskName("task-result-store-id-lookup")
    block_number = 456
    store = InMemoryTaskResultStore[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput]()

    successful_execution = build_nexus_task_result(
        executor_payload="input-3",
        output=9,
        block_number=block_number,
        target_hotkey="task-result-storer-neuron",
    )
    executor_failure_execution = cast(
        StoredTaskExecution[DummyExecutorPayload, ExecutorFailureException],
        build_nexus_task_result(
            executor_payload="input-4",
            output=cast(
                DummyExecutorOutput | NexusException,
                ExecutorFailureException(NexusException("executor boom")),
            ),
            block_number=block_number,
            target_hotkey="task-result-storer-neuron",
        ),
    )

    with empty_context_store().create_context() as successful_ctx:
        successful_result = store.add_successful_task_result(
            successful_ctx,
            task_name,
            SuccessfulTaskResultToPersist(
                result=successful_execution,
                executor_public_output="public-output-2",
            ),
        )
    with empty_context_store().create_context() as failure_ctx:
        executor_failure_result = store.add_executor_failure(
            failure_ctx,
            task_name,
            ExecutorFailureTaskResultToPersist(result=executor_failure_execution),
        )

    assert store.get_task_result(task_name=task_name, task_result_id=successful_result.id) == successful_result
    assert (
        store.get_task_result(task_name=task_name, task_result_id=executor_failure_result.id) == executor_failure_result
    )
    assert store.get_task_result(task_name=task_name, task_result_id=successful_result.id) is successful_result
    assert (
        store.get_task_result(task_name=task_name, task_result_id=executor_failure_result.id) is executor_failure_result
    )
    assert isinstance(
        store.get_task_result(task_name=task_name, task_result_id=successful_result.id),
        SuccessfulTaskResult,
    )
    assert isinstance(
        store.get_task_result(task_name=task_name, task_result_id=executor_failure_result.id), ExecutorFailureTaskResult
    )
    assert store.get_task_result(task_name=task_name, task_result_id=successful_result.id) != executor_failure_result
    assert store.get_task_result(task_name=task_name, task_result_id=executor_failure_result.id) != successful_result


def test_executor_failure_task_result_storer_emits_error_when_store_write_raises(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    task_name = NexusTaskName("task-result-storer-store-write-failure")
    store_write_failure = ExecutorFailureStoreWriteException("failed to persist executor failure")
    storer = ExecutorFailureTaskResultStorer[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ](
        NodeId("executor-failure-task-result-storer"),
        name=task_name,
        task_result_store_provider=TaskResultStoreProviderDouble(
            ExplodingExecutorFailureTaskResultStore(store_write_failure)
        ),
    )
    setup = transform_actor_test_setup_factory(storer)

    executor_failure_execution = cast(
        StoredTaskExecution[DummyExecutorPayload, ExecutorFailureException],
        build_nexus_task_result(
            executor_payload="input-5",
            output=cast(
                DummyExecutorOutput | NexusException,
                ExecutorFailureException(NexusException("executor boom")),
            ),
            block_number=789,
            target_hotkey="task-result-storer-neuron",
        ),
    )

    with setup.running():
        setup.send(
            input_payload=ExecutorFailureTaskResultToPersist(result=executor_failure_execution),
        )
        wait_until(lambda: len(setup.error_collector.received_events) == 1)

    assert len(setup.processed_collector.received_events) == 0
    assert len(setup.error_collector.received_events) == 1
    assert setup.error_collector.received_events[0].payload is store_write_failure
