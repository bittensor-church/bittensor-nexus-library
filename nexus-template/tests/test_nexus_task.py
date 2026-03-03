# pyright: basic

from datetime import UTC, datetime, timedelta

import pytest
from nexus_task_test_setup import (
    DummyExecutorCommunicator,
    DummyPayloadCreator,
    DummyProcessedInput,
    DummyTaskInput,
    NexusTaskTestSetup,
    NexusTaskTestSetupFactory,
    NoopRouter,
)
from utils import build_neuron, wait_until

from nexus.actors.retry_strategy import RetriesExhaustedException, RetryStrategy
from nexus.core.runtime.context_store_types import ContextId
from nexus.core.runtime.nexus_task_types import TaskResultId
from nexus.core.runtime.task_result_store import SingleTaskResult
from nexus.utils.chain import get_epoch_containing_block
from nexus.utils.exceptions import ExecutorFailureException
from nexus.utils.types import BlockNumber, NetUid

DEFAULT_TIMESTAMP_TOLERANCE = timedelta(seconds=10)


def assert_stored_task_result(
    *,
    setup: NexusTaskTestSetup,
    stored_result: SingleTaskResult[DummyProcessedInput],
    input_payload: DummyTaskInput,
    expected_failure: bool = False,
    block_number: int,
    expected_result_id: TaskResultId | None = None,
) -> None:
    """Verify one stored task result entry for success output or executor failure output."""
    if expected_result_id is not None:
        # Identity linkage: this stored entry should be the one identified by the emitted task-result id.
        assert stored_result.id == expected_result_id

    # Expected transformation contract comes from the dummy actors used in setup, not hardcoded test literals.
    expected_executor_payload = setup.payload_creator.to_executor_payload(input_payload)
    processed_result = stored_result.result.executor_output

    # Stored routed input should preserve request identity and contain transformed payload.
    assert processed_result.input.input == expected_executor_payload

    # Stored executor output should match expected success output or represent executor failure.
    if expected_failure:
        assert isinstance(processed_result.output, ExecutorFailureException)
    else:
        expected_task_output = setup.executor_communicator.to_executor_output(expected_executor_payload)
        assert processed_result.output == expected_task_output

    # Routing metadata should target the deterministic dummy neuron configured in test setup.
    assert processed_result.input.target.hotkey == "nexus-task-test-neuron"

    timestamped = stored_result.result
    # Chain-time linkage and temporal ordering must be valid for persisted timestamps.
    assert timestamped.block_at_finish.block_number == BlockNumber(block_number)
    assert timestamped.processing_finished >= timestamped.processing_started

    now = datetime.now(tz=UTC)
    # Persisted timestamps should be recent relative to test execution time.
    assert now - timestamped.processing_started <= DEFAULT_TIMESTAMP_TOLERANCE
    assert now - timestamped.processing_finished <= DEFAULT_TIMESTAMP_TOLERANCE


def assert_successful_task_result(
    *,
    setup: NexusTaskTestSetup,
    input_ctx_id: ContextId,
    block_number: int,
    expected_stored_results_for_epoch: int = 1,
) -> tuple[SingleTaskResult[DummyProcessedInput], TaskResultId]:
    """Validate emitted-success linkage and return the matched stored success entry + emitted id."""
    # Runtime outcome: no terminal retries-exhausted error and exactly one emitted successful task-result event.
    assert len(setup.error_collector.received_events) == 0
    assert len(setup.result_collector.received_events) == 1
    result_event = setup.result_collector.received_events[0]
    # Emitted event must belong to the same processing context and carry a TaskResultId payload.
    assert result_event.ctx_id == input_ctx_id
    emitted_result_id = result_event.payload

    # Store lookup is scoped to expected epoch and task identity.
    epoch = get_epoch_containing_block(BlockNumber(block_number), netuid=NetUid(1))
    stored_results = setup.task_result_store.get_tasks_for_epoch(setup.task.name, epoch)
    # Expected number of persisted entries may vary by scenario (e.g. failure + retry success).
    assert len(stored_results) == expected_stored_results_for_epoch

    # Emitted result id must map to exactly one stored entry.
    stored_result = next((entry for entry in stored_results if entry.id == emitted_result_id), None)
    assert stored_result is not None
    return stored_result, emitted_result_id


def test_nexus_task_happy_path_routes_input_to_task_result_id(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: with a BlockBeat available, an input should flow through retry -> payload creator -> router ->
    # communicator -> timestamper -> task-result-storer and emit exactly one TaskResultId.
    setup = nexus_task_test_setup_factory()
    block_number = 123
    input_payload = DummyTaskInput(
        request_id="req-1",
        payload_text="hello",
    )

    with setup.running():
        setup.send_block_beat(block_number=block_number)
        input_ctx_id = setup.send_input(input_payload=input_payload)
        wait_until(lambda: len(setup.result_collector.received_events) == 1, timeout=2.0)

    stored_success_result, emitted_result_id = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
        expected_result_id=emitted_result_id,
    )


def test_nexus_task_waits_for_block_beat_before_emitting_result(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: communicator output is produced first, but no TaskResultId should be emitted until a BlockBeat arrives
    # at the NexusTask block_beat sink.
    setup = nexus_task_test_setup_factory()
    input_payload = DummyTaskInput(
        request_id="req-waits-for-block-beat",
        payload_text="hello",
    )
    block_number = 222

    with setup.running():
        input_ctx_id = setup.send_input(input_payload=input_payload)
        with pytest.raises(AssertionError):
            wait_until(
                lambda: len(setup.result_collector.received_events) == 1,
                timeout=0.2,
                interval=0.05,
            )
        assert len(setup.result_collector.received_events) == 0

        setup.send_block_beat(block_number=block_number)
        wait_until(lambda: len(setup.result_collector.received_events) == 1, timeout=2.0)

    stored_success_result, emitted_result_id = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
        expected_result_id=emitted_result_id,
    )


def test_nexus_task_retries_after_payload_creator_failure(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: payload creator emits an error and NexusTask forwards it to retry.failed_attempt, causing the next
    # attempt to be re-issued via retry.next_attempt.
    payload_creator = DummyPayloadCreator(
        "nexus-task-test-fails-first-payload-creator",
        fail_first_n_attempts=1,
    )
    setup = nexus_task_test_setup_factory(payload_creator=payload_creator)
    input_payload = DummyTaskInput(
        request_id="req-retry-on-payload-failure",
        payload_text="hello",
    )
    block_number = 321

    with setup.running():
        setup.send_block_beat(block_number=block_number)
        input_ctx_id = setup.send_input(input_payload=input_payload)
        wait_until(lambda: len(setup.result_collector.received_events) == 1, timeout=2.0)

    stored_success_result, emitted_result_id = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
        expected_result_id=emitted_result_id,
    )
    assert payload_creator.attempts_by_ctx[input_ctx_id] == 2


def test_nexus_task_retries_after_router_failure(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: router emits an error and NexusTask forwards it to retry.failed_attempt so processing can continue
    # from the retry strategy instead of terminating immediately.
    router = NoopRouter(
        "nexus-task-test-fails-first-router",
        target=build_neuron(uid=1, hotkey="nexus-task-test-neuron", validator_permit=False),
        fail_first_n_attempts=1,
    )
    setup = nexus_task_test_setup_factory(router=router)
    input_payload = DummyTaskInput(
        request_id="req-retry-on-router-failure",
        payload_text="hello",
    )
    block_number = 654

    with setup.running():
        setup.send_block_beat(block_number=block_number)
        input_ctx_id = setup.send_input(input_payload=input_payload)
        wait_until(lambda: len(setup.result_collector.received_events) == 1, timeout=2.0)

    stored_success_result, emitted_result_id = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
        expected_result_id=emitted_result_id,
    )
    assert router.attempts_by_ctx[input_ctx_id] == 2


def test_nexus_task_retries_after_communicator_internal_error(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: executor communicator emits a framework/internal error and NexusTask routes it into retry.failed_attempt
    # to trigger retry scheduling.
    communicator = DummyExecutorCommunicator(
        "nexus-task-test-fails-first-communicator",
        fail_first_n_internal_errors=1,
    )
    setup = nexus_task_test_setup_factory(executor_communicator=communicator)
    input_payload = DummyTaskInput(
        request_id="req-retry-on-communicator-internal-error",
        payload_text="hello",
    )
    block_number = 987

    with setup.running():
        setup.send_block_beat(block_number=block_number)
        input_ctx_id = setup.send_input(input_payload=input_payload)
        wait_until(lambda: len(setup.result_collector.received_events) == 1, timeout=2.0)

    stored_success_result, emitted_result_id = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
        expected_result_id=emitted_result_id,
    )
    assert communicator.attempts_by_ctx[input_ctx_id] == 2


def test_nexus_task_retries_after_executor_failure_result_is_stored(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: communicator returns an executor-side failure result, TaskResultStorer persists it and raises
    # RetryTaskAfterExecutorFailureException, and NexusTask wiring sends that error back into retry.failed_attempt.
    communicator = DummyExecutorCommunicator(
        "nexus-task-test-fails-first-with-executor-failure",
        fail_first_n_executor_failures=1,
    )
    setup = nexus_task_test_setup_factory(executor_communicator=communicator)
    input_payload = DummyTaskInput(
        request_id="req-retry-after-executor-failure-result",
        payload_text="hello",
    )
    block_number = 741

    with setup.running():
        input_ctx_id = setup.send_input(input_payload=input_payload)
        setup.send_block_beat(block_number=block_number)
        wait_until(lambda: len(setup.result_collector.received_events) == 1, timeout=2.0)

    stored_success_result, emitted_result_id = assert_successful_task_result(
        setup=setup,
        input_ctx_id=input_ctx_id,
        block_number=block_number,
        expected_stored_results_for_epoch=2,
    )
    assert_stored_task_result(
        setup=setup,
        stored_result=stored_success_result,
        input_payload=input_payload,
        block_number=block_number,
        expected_result_id=emitted_result_id,
    )
    assert communicator.attempts_by_ctx[input_ctx_id] == 2

    epoch = get_epoch_containing_block(BlockNumber(block_number), netuid=NetUid(1))
    stored_results = setup.task_result_store.get_tasks_for_epoch(setup.task.name, epoch)
    assert len(stored_results) == 2
    failed_stored_result = next(
        (
            entry
            for entry in stored_results
            if isinstance(entry.result.executor_output.output, ExecutorFailureException)
        ),
        None,
    )
    assert failed_stored_result is not None
    assert_stored_task_result(
        setup=setup,
        stored_result=failed_stored_result,
        input_payload=input_payload,
        expected_failure=True,
        block_number=block_number,
    )


def test_nexus_task_emits_retries_exhausted_when_max_attempts_are_hit(
    nexus_task_test_setup_factory: NexusTaskTestSetupFactory,
) -> None:
    # Scenario: repeated failures from internal stages reach retry.max_attempts and NexusTask exposes the terminal
    # RetriesExhaustedException on task.error.
    max_attempts = 2
    retry = RetryStrategy[DummyTaskInput](
        "nexus-task-test-retry-exhausted",
        max_attempts=max_attempts,
        delay=timedelta(milliseconds=5),
    )
    payload_creator = DummyPayloadCreator(
        "nexus-task-test-always-failing-payload-creator",
        fail_first_n_attempts=max_attempts,
    )
    setup = nexus_task_test_setup_factory(retry=retry, payload_creator=payload_creator)
    input_payload = DummyTaskInput(
        request_id="req-retries-exhausted",
        payload_text="hello",
    )

    with setup.running():
        input_ctx_id = setup.send_input(input_payload=input_payload)
        wait_until(lambda: len(setup.error_collector.received_events) == 1, timeout=2.0)

    assert len(setup.result_collector.received_events) == 0
    assert len(setup.error_collector.received_events) == 1

    exhausted_event = setup.error_collector.received_events[0]
    assert exhausted_event.ctx_id == input_ctx_id
    assert isinstance(exhausted_event.payload, RetriesExhaustedException)
    assert f"All {max_attempts} retry attempts exhausted" in str(exhausted_event.payload)
    assert payload_creator.attempts_by_ctx[input_ctx_id] == max_attempts
