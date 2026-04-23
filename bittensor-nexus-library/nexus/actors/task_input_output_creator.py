from dataclasses import dataclass
from typing import override

from pydantic import BaseModel

from nexus.actors.payload_creator import PayloadCreator
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.core.runtime.nexus_task_types import TaskResultId
from nexus.core.runtime.task_result_store import SuccessfulTaskResult


@dataclass(frozen=True)
class TaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
    """Executor-ready details extracted from one successful stored task result."""

    task_result_id: TaskResultId
    task_input: ExecutorPayload
    task_output: ExecutorOutput
    task_public_output: ExecutorPublicOutput


class BatchedTaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](BaseModel):
    """A batch of executor-ready details derived from successful task results."""

    task_input_outputs: tuple[TaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...]


class TaskInputOutputCreator[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    PayloadCreator[
        tuple[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...],
        BatchedTaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ],
    ActorBuilder,
):
    """PayloadCreator for validation pipelines. Repackages a batch of stored mining task results
    into a structured format for the validation executor, extracting each result's input, output,
    and public output. Expects only successful results — failed ones should be filtered upstream.

    sink input: batch of SingleTaskResult from the sampler
    source created_payload: BatchedTaskInputOutput with aligned input/output pairs per task
    source error: creation failures
    """

    def __init__(self, _id: str) -> None:
        super().__init__(_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return TaskInputOutputCreatorActor[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )


class TaskInputOutputCreatorActor[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    TransformActor[
        tuple[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...],
        BatchedTaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ]
):
    """Transform successful task result batches into executor input/output/public-output payloads."""

    def __init__(
        self,
        *,
        spec: TaskInputOutputCreator[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)

    @override
    def _transform(
        self,
        ctx: Context,
        payload: tuple[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...],
    ) -> BatchedTaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        del ctx
        transformed: list[TaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]] = []
        for task_result in payload:
            transformed.append(
                TaskInputOutput(
                    task_result_id=task_result.id,
                    task_input=task_result.executor_payload,
                    task_output=task_result.executor_output,
                    task_public_output=task_result.executor_public_output,
                )
            )
        return BatchedTaskInputOutput(task_input_outputs=tuple(transformed))
