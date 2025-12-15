from typing import override

from nexus.context_store import ContextId
from nexus.core.dsl.nodes import Fork
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import ForkActor
from nexus.core.runtime.events import PipeToBus


class UppercaseIfEven(Fork[str, str, str], ActorBuilder):
    def __init__(self, gid_prefix: str | None = None) -> None:
        super().__init__(gid_prefix=gid_prefix)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus) -> UppercaseIfEvenActor:
        return UppercaseIfEvenActor(spec=self, pipe_to_bus=pipe_to_bus)


class UppercaseIfEvenActor(ForkActor[str, str, str]):
    def __init__(self, *, spec: UppercaseIfEven, pipe_to_bus: PipeToBus) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus)

    def _process(self, ctx: ContextId, payload: str) -> tuple[str, None] | tuple[None, str]:
        if len(payload) % 2 == 0:
            return payload.upper(), None
        else:
            return None, payload
