from typing import override

from nexus.core.runtime.context_store import ContextId
from nexus.core.dsl.nodes import DoubleTransform
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import DoubleTransformActor
from nexus.core.runtime.events import PipeToBus


class UppercaseInputLowercaseOutput(DoubleTransform[str, str, str, str], ActorBuilder):
    def __init__(self, _id: str) -> None:
        super().__init__(_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus) -> UppercaseInputLowercaseOutputActor:
        return UppercaseInputLowercaseOutputActor(spec=self, pipe_to_bus=pipe_to_bus)


class UppercaseInputLowercaseOutputActor(DoubleTransformActor[str, str, str, str]):
    def __init__(self, *, spec: UppercaseInputLowercaseOutput, pipe_to_bus: PipeToBus) -> None:
        super().__init__(
            name=spec.id, input_spec=spec.input_transform, output_spec=spec.output_transform, pipe_to_bus=pipe_to_bus
        )

    @override
    def _transform_input(self, ctx: ContextId, payload: str) -> str:
        return payload.upper()

    @override
    def _transform_output(self, ctx: ContextId, payload: str) -> str:
        return payload.lower()
