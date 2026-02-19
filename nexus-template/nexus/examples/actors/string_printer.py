from typing import override

from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.dsl.nodes import Sink
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import ConsumerActor
from nexus.core.runtime.events import PipeToBus


class StringPrinter(Sink[str], ActorBuilder):
    """
    An actor that prints incoming strings to the console.
    """

    def __init__(self, _id: str) -> None:
        super().__init__(_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> StringPrinterActor:
        return StringPrinterActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class StringPrinterActor(ConsumerActor[str]):
    def __init__(self, *, spec: StringPrinter, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)

    def _consume(self, ctx: Context, payload: str) -> None:
        print(payload)
