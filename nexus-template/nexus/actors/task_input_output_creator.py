from dataclasses import dataclass
from typing import override

from nexus.actors.payload_creator import PayloadCreator
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.core.runtime.nexus_task_types import TaskResultId
from nexus.core.runtime.task_result_store import SingleTaskResult
from nexus.utils.exceptions import InternalFrameworkException, NexusException


@dataclass(frozen=True)
class TaskInputOutput[ExecutorPayload, Output]:
    """Executor-ready task result details derived from one stored task result."""

    task_result_id: TaskResultId
    task_input: ExecutorPayload
    task_output: Output


class TaskInputOutputCreator[ExecutorPayload, Output](
    PayloadCreator[
        tuple[SingleTaskResult[ExecutorPayload, Output], ...],
        tuple[TaskInputOutput[ExecutorPayload, Output], ...],
    ],
    ActorBuilder,
):
    """Converts batches of stored task results into task id/input/output tuples."""

    def __init__(self, _id: str) -> None:
        super().__init__(_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return TaskInputOutputCreatorActor[ExecutorPayload, Output](
            spec=self,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )


class TaskInputOutputCreatorActor[ExecutorPayload, Output](
    TransformActor[
        tuple[SingleTaskResult[ExecutorPayload, Output], ...],
        tuple[TaskInputOutput[ExecutorPayload, Output], ...],
    ]
):
    """Actor for transforming task result batches into executor input/output payloads."""

    def __init__(
        self,
        *,
        spec: TaskInputOutputCreator[ExecutorPayload, Output],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)

    @override
    def _transform(
        self,
        ctx: Context,
        payload: tuple[SingleTaskResult[ExecutorPayload, Output], ...],
    ) -> tuple[TaskInputOutput[ExecutorPayload, Output], ...]:
        transformed: list[TaskInputOutput[ExecutorPayload, Output]] = []
        for task_result in payload:
            task_output = task_result.executor_output
            if isinstance(task_output, NexusException):
                raise InternalFrameworkException(
                    "failed task results should have been filtered out before reaching TaskInputOutputCreatorActor"
                )
            transformed.append(
                TaskInputOutput[ExecutorPayload, Output](
                    task_result_id=task_result.id,
                    task_input=task_result.executor_payload,
                    task_output=task_output,
                )
            )
        return tuple(transformed)
