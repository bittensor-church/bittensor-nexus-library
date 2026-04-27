from typing import override

from nexus.core.dsl.nodes import Fork
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import ForkActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus


class UppercaseIfEven(Fork[str, str, str], ActorBuilder):
    """Example fork. Uppercases strings with even length to `left`, passes odd-length strings unchanged to `right`.

    sink sink: string to process
    source left: uppercased string (even-length inputs)
    source right: original string (odd-length inputs)
    """

    def __init__(self, _id: str) -> None:
        super().__init__(_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> UppercaseIfEvenActor:
        return UppercaseIfEvenActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class UppercaseIfEvenActor(ForkActor[str, str, str]):
    def __init__(self, *, spec: UppercaseIfEven, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)

    def _process(self, ctx: Context, payload: str) -> tuple[str, None] | tuple[None, str]:
        if len(payload) % 2 == 0:
            return payload.upper(), None
        else:
            return None, payload
