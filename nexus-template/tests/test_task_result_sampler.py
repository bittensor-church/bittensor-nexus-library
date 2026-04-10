# pyright: basic

from transform_test_utils import TransformActorTestSetupFactory
from utils import (
    InMemoryTestTaskResultStoreProvider,
    build_nexus_task_result,
    empty_context_store,
    store_executor_failure_task_result,
    store_successful_task_result,
    wait_until,
)

from nexus.actors.task_result_sampler import SuccessfulTaskResultSampler
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.task_result_store import SuccessfulTaskResult
from nexus.utils.exceptions import ExecutorFailureException, InternalFrameworkException, NexusException

type DummyExecutorPayload = str
type DummyExecutorOutput = int
type DummyExecutorPublicOutput = str


def test_successful_task_result_sampler_actor_emits_singleton_batch_for_each_task_result(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    sampler = SuccessfulTaskResultSampler[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput](
        "successful-task-result-sampler"
    )
    setup = transform_actor_test_setup_factory(sampler)

    task_result_store_provider = InMemoryTestTaskResultStoreProvider[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ]()
    task_result_store = task_result_store_provider.get_task_result_store()
    task_name = NexusTaskName("successful-task-result-sampler")
    context_store = empty_context_store()
    first_result = store_successful_task_result(
        context_store=context_store,
        task_result_store=task_result_store,
        task_name=task_name,
        result=build_nexus_task_result(
            executor_payload="payload-1",
            output=1,
            block_number=100,
            target_hotkey="hotkey-1",
        ),
        executor_public_output="public-1",
    )
    second_result = store_successful_task_result(
        context_store=context_store,
        task_result_store=task_result_store,
        task_name=task_name,
        result=build_nexus_task_result(
            executor_payload="payload-2",
            output=2,
            block_number=101,
            target_hotkey="hotkey-2",
        ),
        executor_public_output="public-2",
    )
    sampled_batch: tuple[
        SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput],
        ...,
    ]

    with setup.running():
        first_ctx_id = setup.send(input_payload=first_result)
        second_ctx_id = setup.send(input_payload=second_result)
        wait_until(lambda: len(setup.processed_collector.received_events) == 2, timeout=2.0)

    assert len(setup.error_collector.received_events) == 0
    emitted = setup.processed_collector.received_events
    emitted_by_context = {event.ctx_id: event.payload for event in emitted}
    sampled_batch = emitted_by_context[first_ctx_id]
    assert emitted_by_context == {
        first_ctx_id: (first_result,),
        second_ctx_id: (second_result,),
    }
    assert sampled_batch == (first_result,)


def test_successful_task_result_sampler_actor_emits_framework_error_for_executor_failure_payload(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    sampler = SuccessfulTaskResultSampler[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput](
        "successful-task-result-sampler"
    )
    setup = transform_actor_test_setup_factory(sampler)

    task_result_store_provider = InMemoryTestTaskResultStoreProvider[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ]()
    task_result_store = task_result_store_provider.get_task_result_store()
    task_name = NexusTaskName("successful-task-result-sampler")
    context_store = empty_context_store()
    executor_failure_result = store_executor_failure_task_result(
        context_store=context_store,
        task_result_store=task_result_store,
        task_name=task_name,
        result=build_nexus_task_result(
            executor_payload="payload-failed",
            output=ExecutorFailureException(NexusException("executor failed")),
            block_number=100,
            target_hotkey="hotkey-failed",
        ),
    )

    with setup.running():
        ctx_id = setup.send(input_payload=executor_failure_result)  # pyright: ignore[reportArgumentType]
        wait_until(lambda: len(setup.error_collector.received_events) == 1, timeout=2.0)

    assert len(setup.processed_collector.received_events) == 0
    error_event = setup.error_collector.received_events[0]
    assert error_event.ctx_id == ctx_id
    assert isinstance(error_event.payload, InternalFrameworkException)
    assert "SuccessfulTaskResult" in str(error_event.payload)
