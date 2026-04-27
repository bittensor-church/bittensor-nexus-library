from typing import override

from nexus.core.dsl.nodes import Transform
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.utils.exceptions import NexusException


class EvenSucks(NexusException):
    pass


class UppercaseOrError(Transform[str, str], ActorBuilder):
    """Example transform. Uppercases strings with an odd character count, errors on even.

    sink input: string to transform
    source ok: uppercased string (odd-length inputs only)
    source error: EvenSucks for even-length inputs
    """

    def __init__(self, _id: str) -> None:
        super().__init__(_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> UppercaseOrErrorActor:
        return UppercaseOrErrorActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class UppercaseOrErrorActor(TransformActor[str, str]):
    def __init__(self, *, spec: UppercaseOrError, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)

    @override
    def _transform(self, ctx: Context, payload: str) -> str:
        if len(payload) % 2 == 0:
            raise EvenSucks(f"The input string has an even number of characters: {len(payload)}")
        else:
            return payload.upper()
