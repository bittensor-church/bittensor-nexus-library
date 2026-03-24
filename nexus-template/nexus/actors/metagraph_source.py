from __future__ import annotations

from dataclasses import dataclass
from typing import Any, override

from pylon_client.artanis.v1 import Neuron

from nexus.actors.pylon_client_provider import DEFAULT_PYLON_CLIENT_PROVIDER, PylonClientProvider
from nexus.core.dsl.nodes import NodeSinks, NodeSources, Sink, SinkName, Source, SourceName, Transform
from nexus.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent
from nexus.utils.immutable_map import ImmutableMap
from nexus.utils.netuid import load_required_netuid_from_env, validate_netuid
from nexus.utils.types import Hotkey, NetUid


class NeuronMap(ImmutableMap[Hotkey, Neuron]):
    pass


@dataclass(frozen=True)
class TriggeredMetagraph[Trigger]:
    trigger: Trigger
    neurons: NeuronMap


class MetagraphSource[Trigger](Transform[Trigger, TriggeredMetagraph[Trigger]], ActorBuilder):
    trigger: Sink[Trigger]
    metagraph: Source[TriggeredMetagraph[Trigger]]

    netuid: NetUid
    pylon_client_provider: PylonClientProvider

    def __init__(
        self,
        _id: str,
        *,
        netuid: NetUid | None = None,
        pylon_client_provider: PylonClientProvider | None = None,
    ) -> None:
        super().__init__(_id)
        self.netuid = validate_netuid(netuid) if netuid is not None else load_required_netuid_from_env()
        self.pylon_client_provider = pylon_client_provider or DEFAULT_PYLON_CLIENT_PROVIDER
        self.trigger = self.sink
        self.metagraph = self.ok

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={SinkName("trigger"): self.trigger})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("metagraph"): self.metagraph,
                SourceName("error"): self.error,
            },
            default_source=self.metagraph,
        )

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return _PlaceholderMetagraphSourceActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class _PlaceholderMetagraphSourceActor[Trigger](Actor):
    spec: MetagraphSource[Trigger]

    def __init__(
        self,
        *,
        spec: MetagraphSource[Trigger],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.spec.trigger: self._handle}

    def _handle(self, _: Context, event: ReceiveEvent[Trigger]) -> MessagesToSend:
        raise NotImplementedError(f"{self.spec.id} runtime is not implemented yet; received trigger {event.payload!r}")
