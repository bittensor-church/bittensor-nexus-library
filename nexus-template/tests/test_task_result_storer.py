# pyright: basic

from datetime import UTC, datetime

from transform_test_utils import TransformActorTestSetupFactory
from utils import InMemoryTestTaskResultStoreProvider, dummy_block_beat, get_stored_results_for_block, wait_until

from nexus.actors import Timestamped
from nexus.actors.executor_communicator import ProcessedInput
from nexus.actors.task_result_storer import TaskResultStorer
from nexus.core.dsl.nodes import NodeId
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.utils.exceptions import ExecutorFailureException, NexusException, RetryTaskAfterExecutorFailureException

type StorerInput = str
type StorerOutput = int
type StorerProcessedInput = ProcessedInput[StorerInput, StorerOutput]


def build_timestamped_payload(
    *,
    input_payload: StorerInput,
    output: StorerOutput | NexusException,
    block_number: int,
) -> Timestamped[StorerProcessedInput]:
    return Timestamped(
        executor_output=ProcessedInput(input=input_payload, output=output),
        processing_started=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        processing_finished=datetime(2025, 1, 1, 0, 0, 1, tzinfo=UTC),
        block_at_finish=dummy_block_beat(block_number),
    )


def test_task_result_storer_emits_task_result_id_and_persists_payload(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    task_name = NexusTaskName("task-result-storer-success")
    store_provider = InMemoryTestTaskResultStoreProvider[StorerProcessedInput]()
    storer = TaskResultStorer[StorerInput, StorerOutput](
        NodeId("task-result-storer-success"),
        name=task_name,
        task_result_store_provider=store_provider,
    )
    setup = transform_actor_test_setup_factory(storer)
    block_number = 123
    timestamped_payload = build_timestamped_payload(
        input_payload="input-1",
        output=7,
        block_number=block_number,
    )

    with setup.running():
        ctx_id = setup.send(input_payload=timestamped_payload)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

    assert len(setup.error_collector.received_events) == 0
    result_event = setup.processed_collector.received_events[0]
    assert result_event.ctx_id == ctx_id

    stored_results = get_stored_results_for_block(
        store=store_provider.get_task_result_store(),
        task_name=task_name,
        block_number=block_number,
    )
    assert len(stored_results) == 1
    stored_result = stored_results[0]
    assert stored_result.id == result_event.payload
    assert stored_result.result == timestamped_payload


def test_task_result_storer_persists_executor_failure_and_emits_retry_error(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    task_name = NexusTaskName("task-result-storer-executor-failure")
    store_provider = InMemoryTestTaskResultStoreProvider[StorerProcessedInput]()
    storer = TaskResultStorer[StorerInput, StorerOutput](
        NodeId("task-result-storer-executor-failure"),
        name=task_name,
        task_result_store_provider=store_provider,
    )
    setup = transform_actor_test_setup_factory(storer)
    block_number = 456
    timestamped_payload = build_timestamped_payload(
        input_payload="input-2",
        output=ExecutorFailureException(NexusException("executor boom")),
        block_number=block_number,
    )

    with setup.running():
        ctx_id = setup.send(input_payload=timestamped_payload)
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
    assert stored_result.result == timestamped_payload
    stored_output = stored_result.result.executor_output.output
    assert isinstance(stored_output, ExecutorFailureException)
    assert str(stored_output.executor_error) == "executor boom"
