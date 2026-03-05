from dataclasses import dataclass
from typing import override

from pydantic import BaseModel

from nexus.actors.payload_creator import PayloadCreator
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.core.runtime.nexus_task_types import TaskResultId
from nexus.core.runtime.task_result_store import SingleTaskResult
from nexus.utils.exceptions import InternalFrameworkException, NexusException


@dataclass(frozen=True)
class TaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
    """Executor-ready task result details derived from one stored task result."""

    task_result_id: TaskResultId
    task_input: ExecutorPayload
    task_output: ExecutorOutput
    task_public_output: ExecutorPublicOutput


class BatchedTaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](BaseModel):
    """A batch of task input/output tuples."""

    task_input_outputs: tuple[TaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...]


class TaskInputOutputCreator[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    PayloadCreator[
        tuple[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...],
        BatchedTaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ],
    ActorBuilder,
):
    """Converts batches of stored task results into task id/input/output tuples."""

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
        tuple[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...],
        BatchedTaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ]
):
    """Actor for transforming task result batches into executor input/output payloads."""

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
        payload: tuple[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...],
    ) -> BatchedTaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        transformed: list[TaskInputOutput[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]] = []
        for task_result in payload:
            task_output = task_result.executor_output
            if isinstance(task_output, NexusException):
                raise InternalFrameworkException(
                    "failed task results should have been filtered out before reaching TaskInputOutputCreatorActor"
                )
            task_public_output = task_result.executor_public_output
            if task_public_output is None:
                raise InternalFrameworkException(
                    "successful task results should include executor_public_output before reaching "
                    "TaskInputOutputCreatorActor"
                )
            transformed.append(
                TaskInputOutput(
                    task_result_id=task_result.id,
                    task_input=task_result.executor_payload,
                    task_output=task_output,
                    task_public_output=task_public_output,
                )
            )
        return BatchedTaskInputOutput(task_input_outputs=tuple(transformed))
