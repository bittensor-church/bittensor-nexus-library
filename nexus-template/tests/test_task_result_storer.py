# pyright: basic

from typing import cast

from transform_test_utils import TransformActorTestSetupFactory
from utils import (
    InMemoryTestTaskResultStoreProvider,
    build_nexus_task_result,
    get_stored_results_for_block,
    wait_until,
)

from nexus.actors.task_result_storer import TaskResultStorer
from nexus.core.dsl.nodes import NodeId
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.task_result_store import SingleTaskResult, TaskResultToPersist
from nexus.utils.exceptions import ExecutorFailureException, NexusException, RetryTaskAfterExecutorFailureException

type DummyExecutorPayload = str
type DummyExecutorOutput = int
type DummyExecutorPublicOutput = str


def test_task_result_storer_emits_task_result_and_persists_payload(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    task_name = NexusTaskName("task-result-storer-success")
    store_provider = InMemoryTestTaskResultStoreProvider[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ]()
    storer = TaskResultStorer[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput](
        NodeId("task-result-storer-success"),
        name=task_name,
        task_result_store_provider=store_provider,
    )
    setup = transform_actor_test_setup_factory(storer)
    block_number = 123
    nexus_task_result = build_nexus_task_result(
        executor_payload="input-1",
        output=7,
        block_number=block_number,
        target_hotkey="task-result-storer-neuron",
    )
    public_output = "public-output-1"
    task_result_to_persist = TaskResultToPersist[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ](
        result=nexus_task_result,
        executor_public_output=public_output,
    )

    with setup.running():
        ctx_id = setup.send(input_payload=task_result_to_persist)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

    assert len(setup.error_collector.received_events) == 0
    result_event = setup.processed_collector.received_events[0]
    assert result_event.ctx_id == ctx_id
    emitted_result = result_event.payload
    assert isinstance(emitted_result, SingleTaskResult)

    stored_results = get_stored_results_for_block(
        store=store_provider.get_task_result_store(),
        task_name=task_name,
        block_number=block_number,
    )
    assert len(stored_results) == 1
    stored_result = stored_results[0]
    assert emitted_result == stored_result
    assert stored_result.result == nexus_task_result
    assert stored_result.executor_public_output == public_output
    assert (
        store_provider.get_task_result_store().get_task_result(
            task_name=task_name,
            task_result_id=emitted_result.id,
        )
        == emitted_result
    )


def test_task_result_storer_persists_executor_failure_and_emits_retry_error(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    task_name = NexusTaskName("task-result-storer-executor-failure")
    store_provider = InMemoryTestTaskResultStoreProvider[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ]()
    storer = TaskResultStorer[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput](
        NodeId("task-result-storer-executor-failure"),
        name=task_name,
        task_result_store_provider=store_provider,
    )
    setup = transform_actor_test_setup_factory(storer)
    block_number = 456
    failed_output = cast(
        DummyExecutorOutput | NexusException,
        ExecutorFailureException(NexusException("executor boom")),
    )
    nexus_task_result = build_nexus_task_result(
        executor_payload="input-2",
        output=failed_output,
        block_number=block_number,
        target_hotkey="task-result-storer-neuron",
    )
    task_result_to_persist = TaskResultToPersist[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ](
        result=nexus_task_result,
        executor_public_output=None,
    )

    with setup.running():
        ctx_id = setup.send(input_payload=task_result_to_persist)
        wait_until(lambda: len(setup.error_collector.received_events) == 1, timeout=2.0)

    assert len(setup.processed_collector.received_events) == 0
    error_event = setup.error_collector.received_events[0]
    assert error_event.ctx_id == ctx_id
    assert isinstance(error_event.payload, RetryTaskAfterExecutorFailureException)
    assert isinstance(error_event.payload.__cause__, ExecutorFailureException)

    stored_results = get_stored_results_for_block(
        store=store_provider.get_task_result_store(),
        task_name=task_name,
        block_number=block_number,
    )
    assert len(stored_results) == 1
    stored_result = stored_results[0]
    assert stored_result.result == nexus_task_result
    assert stored_result.executor_public_output is None
    stored_output = stored_result.result.executor_output.output
    assert isinstance(stored_output, ExecutorFailureException)
    assert str(stored_output.executor_error) == "executor boom"
