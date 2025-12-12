from typing import override

from context_store import ContextId
from piping.dsl import Transform
from runtime.actor import ActorBuilder, TransformActor
from runtime.events import PipeToBus


class EvenSucks(Exception):
    pass


class UppercaseOrError(Transform[str, str], ActorBuilder):
    def __init__(self, name: str | None = None) -> None:
        super().__init__(name=name)

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
