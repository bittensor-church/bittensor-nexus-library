from typing import override

from nexus.core.dsl.nodes import DoubleTransform
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import DoubleTransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus


class CaseModifier[T](DoubleTransform[str, str, str, str], ActorBuilder):
    def __init__(self, _id: str) -> None:
        super().__init__(_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore):
        return CaseModifierActor[T](spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class CaseModifierActor[T](DoubleTransformActor[str, str, str, str]):
    def __init__(self, *, spec: CaseModifier[T], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(
            name=spec.id,
            input_spec=spec.input_transform,
            output_spec=spec.output_transform,
            pipe_to_bus=pipe_to_bus,
            context_store=context_store,
        )

    @override
    def _transform_input(self, ctx: Context, payload: str) -> str:
        return payload.lower()

    @override
    def _transform_output(self, ctx: Context, payload: str) -> str:
        return payload.upper()
