from typing import override

from nexus.core.dsl.nodes import Transform
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus


class Stringify[T](Transform[T, str], ActorBuilder):
    """Example transform. Converts any input to its string representation.

    sink input: value to stringify
    source ok: stringified value
    source error: transform failures
    """

    def __init__(self, _id: str) -> None:
        super().__init__(_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore):
        return StringifyActor[T](spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class StringifyActor[T](TransformActor[T, str]):
    def __init__(self, *, spec: Stringify[T], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)

    @override
    def _transform(self, ctx: Context, payload: T) -> str:
        return str(payload)
