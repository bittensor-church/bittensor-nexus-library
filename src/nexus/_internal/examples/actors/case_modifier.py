from typing import override

from nexus._internal.core.dsl.nodes import DoubleTransform
from nexus._internal.core.runtime.actor import ActorBuilder
from nexus._internal.core.runtime.actor_patterns import DoubleTransformActor
from nexus._internal.core.runtime.context_store import Context, ContextStore
from nexus._internal.core.runtime.events import PipeToBus


class CaseModifier[T](DoubleTransform[str, str, str, str], ActorBuilder):
    """
    Example double transform. Lowercases strings on the input path, uppercases on the output path.

    sink input_sink: string to lowercase
    sink output_sink: string to uppercase
    source input_ok: lowercased string
    source input_error: input transform failures
    source output_ok: uppercased string
    source output_error: output transform failures
    """

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
