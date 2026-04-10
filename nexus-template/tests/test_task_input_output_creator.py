# pyright: basic

from transform_test_utils import TransformActorTestSetupFactory
from utils import InMemoryTestTaskResultStoreProvider, build_nexus_task_result, store_successful_task_result, wait_until

from nexus.actors.task_input_output_creator import BatchedTaskInputOutput, TaskInputOutput, TaskInputOutputCreator
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.task_result_store import SuccessfulTaskResult

type DummyExecutorPayload = str
type DummyExecutorOutput = int
type DummyExecutorPublicOutput = str


def test_task_input_output_creator_transforms_batch_into_task_result_id_input_and_output(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    creator = TaskInputOutputCreator[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ]("task-input-output-creator")
    setup = transform_actor_test_setup_factory(creator)
    task_name = NexusTaskName("task-input-output-creator")

    task_result_store_provider = InMemoryTestTaskResultStoreProvider[
        DummyExecutorPayload,
        DummyExecutorOutput,
        DummyExecutorPublicOutput,
    ]()
    task_result_store = task_result_store_provider.get_task_result_store()

    single_task_result_1 = store_successful_task_result(
        context_store=setup.runtime.context_store,
        task_result_store=task_result_store,
        task_name=task_name,
        result=build_nexus_task_result(
            executor_payload="task-input-1",
            output=7,
            block_number=123,
            target_hotkey="task-input-output-creator-neuron-1",
        ),
        executor_public_output="public-output-1",
    )
    single_task_result_2 = store_successful_task_result(
        context_store=setup.runtime.context_store,
        task_result_store=task_result_store,
        task_name=task_name,
        result=build_nexus_task_result(
            executor_payload="task-input-2",
            output=9,
            block_number=123,
            target_hotkey="task-input-output-creator-neuron-2",
        ),
        executor_public_output="public-output-2",
    )
    sampled_batch = (single_task_result_1, single_task_result_2)
    typed_sampled_batch: tuple[
        SuccessfulTaskResult[DummyExecutorPayload, DummyExecutorOutput, DummyExecutorPublicOutput],
        ...,
    ] = sampled_batch

    with setup.running():
        ctx_id = setup.send(input_payload=typed_sampled_batch)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

    assert len(setup.error_collector.received_events) == 0
    result_event = setup.processed_collector.received_events[0]
    assert result_event.ctx_id == ctx_id
    assert result_event.payload == BatchedTaskInputOutput(
        task_input_outputs=(
            TaskInputOutput(
                task_result_id=single_task_result_1.id,
                task_input="task-input-1",
                task_output=7,
                task_public_output="public-output-1",
            ),
            TaskInputOutput(
                task_result_id=single_task_result_2.id,
                task_input="task-input-2",
                task_output=9,
                task_public_output="public-output-2",
            ),
        )
    )
