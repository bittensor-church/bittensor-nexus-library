from typing import override

from nexus.context_store import ContextId
from nexus.core.dsl.nodes import Transform
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.events import PipeToBus


class EvenSucks(Exception):
    pass


class UppercaseOrError(Transform[str, str], ActorBuilder):
    def __init__(self, gid_prefix: str | None = None) -> None:
        super().__init__(gid_prefix=gid_prefix)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus) -> UppercaseOrErrorActor:
        return UppercaseOrErrorActor(spec=self, pipe_to_bus=pipe_to_bus)


class UppercaseOrErrorActor(TransformActor[str, str]):
    def __init__(self, *, spec: UppercaseOrError, pipe_to_bus: PipeToBus) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus)

    @override
    def _transform(self, ctx: ContextId, payload: str) -> str:
        if len(payload) % 2 == 0:
            raise EvenSucks(f'The input string has an even number of characters: {len(payload)}')
        else:
            return payload.upper()
