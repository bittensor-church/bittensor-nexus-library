from typing import override

from nexus.core.runtime.context_store import ContextId
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
    def build_actor(self, *, pipe_to_bus: PipeToBus) -> StringPrinterActor:
        return StringPrinterActor(spec=self, pipe_to_bus=pipe_to_bus)


class StringPrinterActor(ConsumerActor[str]):
    def __init__(self, *, spec: StringPrinter, pipe_to_bus: PipeToBus) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus)

    def _consume(self, ctx: ContextId, payload: str) -> None:
        print(payload)
