from typing import override

from nexus.context_store import ContextId
from nexus.core.dsl.nodes import DoubleTransform
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import DoubleTransformActor
from nexus.core.runtime.events import PipeToBus


class CaseModifier[T](DoubleTransform[str, str, str, str], ActorBuilder):
    def __init__(self, *,
                 gid_prefix: str | None = None,
                 ) -> None:
        super().__init__(gid_prefix=gid_prefix)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus):
        return CaseModifierActor[T](spec=self, pipe_to_bus=pipe_to_bus)


class CaseModifierActor[T](DoubleTransformActor[str, str, str, str]):
    def __init__(self, *, spec: CaseModifier[T], pipe_to_bus: PipeToBus) -> None:
        super().__init__(
            name=spec.gid,
            input_spec=spec.input_transform,
            output_spec=spec.output_transform,
            pipe_to_bus=pipe_to_bus)

    @override
    def _transform_input(self, ctx: ContextId, payload: str) -> str:
        return payload.lower()

    @override
    def _transform_output(self, ctx: ContextId, payload: str) -> str:
        return payload.upper()
