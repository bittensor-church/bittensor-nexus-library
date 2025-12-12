from typing import override

from nexus.context_store import ContextId
from nexus.piping.dsl import Transform
from nexus.runtime.actor import ActorBuilder, TransformActor
from nexus.runtime.events import PipeToBus


class Stringify[T](Transform[T, str], ActorBuilder):
    def __init__(self, *,
                 name: str | None = None,
                 ) -> None:
        super().__init__(name=name)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus):
        return StringifyActor[T](spec=self, pipe_to_bus=pipe_to_bus)


class StringifyActor[T](TransformActor[T, str]):
    def __init__(self, *, spec: Stringify[T], pipe_to_bus: PipeToBus) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus)

    def _transform(self, ctx: ContextId, payload: T) -> str:
        return str(payload)
