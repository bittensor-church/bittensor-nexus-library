from typing import override

from nexus.context_store import ContextId
from nexus.core.dsl.nodes import Transform
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.events import PipeToBus


class Stringify[T](Transform[T, str], ActorBuilder):
    def __init__(self, *,
                 gid_prefix: str | None = None,
                 ) -> None:
        super().__init__(gid_prefix=gid_prefix)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus):
        return StringifyActor[T](spec=self, pipe_to_bus=pipe_to_bus)


class StringifyActor[T](TransformActor[T, str]):
    def __init__(self, *, spec: Stringify[T], pipe_to_bus: PipeToBus) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus)

    def _transform(self, ctx: ContextId, payload: T) -> str:
        return str(payload)
