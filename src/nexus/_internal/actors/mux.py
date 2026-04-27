from typing import Any, cast, override

from nexus._internal.core.dsl.nodes import Node, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus._internal.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus._internal.core.runtime.context_store import Context, ContextStore
from nexus._internal.core.runtime.context_store_types import ContextId
from nexus._internal.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent


class Mux2[Common, Left, Right](Node, ActorBuilder):
    """
    Merges two input streams into a single output.

    sink left: first input stream
    sink right: second input stream
    source out: merged output stream
    """

    left: Sink[Left]
    right: Sink[Right]
    out: Source[Common]

    def __init__(self, _id: str) -> None:
        super().__init__(_id)
        self.left = Sink[Left](f"{self.id}-left", owner_node=self)
        self.right = Sink[Right](f"{self.id}-right", owner_node=self)
        self.out = Source[Common](f"{self.id}-out", owner_node=self)

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(
            sinks={
                SinkName("left"): self.left,
                SinkName("right"): self.right,
            }
        )

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("out"): self.out,
            }
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return Mux2Actor[Common, Left, Right](spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class Mux2Actor[Common, Left, Right](Actor):
    spec: Mux2[Common, Left, Right]

    def __init__(
        self,
        *,
        spec: Mux2[Common, Left, Right],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec.left: self._handle_left,
            self.spec.right: self._handle_right,
        }

    def _handle_left(self, ctx: Context, event: ReceiveEvent[Left]) -> MessagesToSend:
        return self._forward(ctx.id, cast(Common, event.payload))

    def _handle_right(self, ctx: Context, event: ReceiveEvent[Right]) -> MessagesToSend:
        return self._forward(ctx.id, cast(Common, event.payload))

    def _forward(self, ctx_id: ContextId, payload: Common) -> MessagesToSend:
        return (SendEvent(ctx_id=ctx_id, source=self.spec.out, payload=payload),)
