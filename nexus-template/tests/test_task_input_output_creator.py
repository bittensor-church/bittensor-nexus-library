# pyright: basic

from typing import cast

from transform_test_utils import TransformActorTestSetupFactory
from utils import InMemoryTestTaskResultStoreProvider, build_nexus_task_result, store_nexus_task_result, wait_until

from nexus.actors.task_input_output_creator import TaskInputOutput, TaskInputOutputCreator
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.utils.exceptions import ExecutorFailureException, InternalFrameworkException, NexusException

type DummyExecutorPayload = str
type DummyExecutorOutput = int


def test_task_input_output_creator_transforms_batch_into_task_result_id_input_and_output(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    creator = TaskInputOutputCreator[DummyExecutorPayload, DummyExecutorOutput]("task-input-output-creator")
    setup = transform_actor_test_setup_factory(creator)
    task_name = NexusTaskName("task-input-output-creator")

    task_result_store_provider = InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput]()
    task_result_store = task_result_store_provider.get_task_result_store()

    single_task_result_1 = store_nexus_task_result(
        context_store=setup.runtime.context_store,
        task_result_store=task_result_store,
        task_name=task_name,
        result=build_nexus_task_result(
            executor_payload="task-input-1",
            output=7,
            block_number=123,
            target_hotkey="task-input-output-creator-neuron-1",
        ),
    )
    single_task_result_2 = store_nexus_task_result(
        context_store=setup.runtime.context_store,
        task_result_store=task_result_store,
        task_name=task_name,
        result=build_nexus_task_result(
            executor_payload="task-input-2",
            output=9,
            block_number=123,
            target_hotkey="task-input-output-creator-neuron-2",
        ),
    )
    sampled_batch = (single_task_result_1, single_task_result_2)

    with setup.running():
        ctx_id = setup.send(input_payload=sampled_batch)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

    assert len(setup.error_collector.received_events) == 0
    result_event = setup.processed_collector.received_events[0]
    assert result_event.ctx_id == ctx_id
    assert result_event.payload == (
        TaskInputOutput[DummyExecutorPayload, DummyExecutorOutput](
            task_result_id=single_task_result_1.id,
            task_input="task-input-1",
            task_output=7,
        ),
        TaskInputOutput[DummyExecutorPayload, DummyExecutorOutput](
            task_result_id=single_task_result_2.id,
            task_input="task-input-2",
            task_output=9,
        ),
    )


def test_task_input_output_creator_emits_error_for_failed_task_result(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    creator = TaskInputOutputCreator[DummyExecutorPayload, DummyExecutorOutput]("task-input-output-creator")
    setup = transform_actor_test_setup_factory(creator)
    task_name = NexusTaskName("task-input-output-creator")
    task_result_store_provider = InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput]()
    task_result_store = task_result_store_provider.get_task_result_store()
    failed_output = cast(
        DummyExecutorOutput | NexusException,
        ExecutorFailureException(NexusException("executor failed")),
    )
    failed_task_result = store_nexus_task_result(
        context_store=setup.runtime.context_store,
        task_result_store=task_result_store,
        task_name=task_name,
        result=build_nexus_task_result(
            executor_payload="task-input-failed",
            output=failed_output,
            block_number=123,
            target_hotkey="task-input-output-creator-neuron-failed",
        ),
    )

    with setup.running():
        ctx_id = setup.send(input_payload=(failed_task_result,))
        wait_until(lambda: len(setup.error_collector.received_events) == 1, timeout=2.0)

    assert len(setup.processed_collector.received_events) == 0
    error_event = setup.error_collector.received_events[0]
    assert error_event.ctx_id == ctx_id
    assert isinstance(error_event.payload, InternalFrameworkException)
    assert "failed task results should have been filtered out" in str(error_event.payload)
