from __future__ import annotations

import random
from abc import ABC, abstractmethod
from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass
from typing import cast, override

from pydantic import BaseModel
from pylon_client.artanis import NetUid, PylonClient
from pylon_client.artanis.v1 import GetNeuronsResponse, Neuron

from nexus import get_logger
from nexus.actors.pylon_client_provider import PylonClientProvider
from nexus.core.dsl.nodes import (
    NodeSinks,
    NodeSources,
    Sink,
    SinkName,
    Source,
    SourceName,
    Transform,
)
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.utils.exceptions import InternalStateCorruptionException, NoRoutableNeuronsException

type NeuronFilter = Callable[[Sequence[Neuron]], Sequence[Neuron]]


logger = get_logger(__name__)

def keep_all_neurons(neurons: Sequence[Neuron]) -> Sequence[Neuron]:
    return neurons


def miners_only(neurons: Sequence[Neuron]) -> Sequence[Neuron]:
    return [neuron for neuron in neurons if not neuron.validator_permit]


def validators_only(neurons: Sequence[Neuron]) -> Sequence[Neuron]:
    return [neuron for neuron in neurons if neuron.validator_permit]


@dataclass
class Routed[Input]:
    input: Input
    target: Neuron


class NeuronRouter[Input](Transform[Input, Routed[Input]]):
    input: Sink[Input]
    routed: Source[Routed[Input]]

    netuid: int
    neuron_filter: NeuronFilter
    pylon_client_provider: PylonClientProvider

    def __init__(
        self,
        _id: str,
        *,
        netuid: int,
        pylon_client_provider: PylonClientProvider,
        neuron_filter: NeuronFilter = keep_all_neurons,
    ) -> None:
        super().__init__(_id)
        assert netuid >= 0, "netuid must be >= 0"
        self.netuid = netuid
        self.neuron_filter = neuron_filter
        self.pylon_client_provider = pylon_client_provider

        # alias for convenience
        self.input = self.sink
        self.routed = self.ok

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={SinkName("input"): self.input})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("routed"): self.routed,
                SourceName("error"): self.error,
            },
            default_source=self.routed,
        )


class RoundRobinRoutingState(BaseModel):
    routed_count: int


class RoundRobinNeuronRouter[Input](NeuronRouter[Input], ActorBuilder):
    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return RoundRobinNeuronRouterActor[Input](spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class NeuronRouterActor[Input](TransformActor[Input, Routed[Input]], ABC):
    router_spec: NeuronRouter[Input]

    def __init__(
        self,
        *,
        spec: NeuronRouter[Input],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.router_spec = spec

    @override
    def _transform(self, ctx: Context, payload: Input) -> Routed[Input]:
        neurons = self._get_routable_neurons_from_pylon()
        selected_neuron = self.select_neuron(ctx=ctx, neurons=neurons)
        return Routed(
            input=payload,
            target=selected_neuron,
        )

    @abstractmethod
    def select_neuron(self, *, ctx: Context, neurons: Sequence[Neuron]) -> Neuron:
        """
        Pick a Neuron from the given list to process the Input
        """

    def _get_routable_neurons_from_pylon(self) -> list[Neuron]:
        with self.router_spec.pylon_client_provider.get_client() as pylon_client_like:
            # recover the original type for convenience
            pylon_client = cast(PylonClient, pylon_client_like)
            recent_neurons: GetNeuronsResponse = pylon_client.v1.open_access.get_recent_neurons(
                NetUid(self.router_spec.netuid)
            )
            neurons_by_hotkey = recent_neurons.neurons
            filtered_neurons = list(self.router_spec.neuron_filter(list(neurons_by_hotkey.values())))
            if len(filtered_neurons) == 0:
                raise NoRoutableNeuronsException(
                    f"No routable neurons found for netuid={self.router_spec.netuid} in {self.router_spec.id}; "
                    f"neurons={neurons_by_hotkey}."
                )
            return filtered_neurons


class RoundRobinNeuronRouterActor[Input](NeuronRouterActor[Input]):
    """
    A NeuronRouterActor that routes to neurons in a round-robin fashion,
    keeping track of the number of times it has routed.
    For a specific context the order of neurons is random
    for load balancing across contexts, but stable as long
    as the set of neurons doesn't change.
    """
    @override
    def select_neuron(self, *, ctx: Context, neurons: Collection[Neuron]) -> Neuron:
        if len(neurons) == 0:
            raise NoRoutableNeuronsException(
                f"Cannot route input in {self.router_spec.id}: no neurons available"
            )

        ordered_neurons = self._ordered_neurons_for_context(ctx, neurons)

        current_state = self._state_from_context(ctx) or RoundRobinRoutingState(routed_count=0)
        selected_neuron = ordered_neurons[current_state.routed_count % len(ordered_neurons)]

        next_route_number = current_state.routed_count + 1
        ctx.set_user_data(self.router_spec.id, RoundRobinRoutingState(routed_count=next_route_number))

        return selected_neuron

    def _state_from_context(self, ctx: Context) -> RoundRobinRoutingState | None:
        existing = ctx.user_data.get(self.router_spec.id)
        if existing is None:
            return None

        if not isinstance(existing, RoundRobinRoutingState):
            logger.error(
                "Internal state corruption? Unexpected routing state for context "
                f"{ctx.id}; type for key {self.router_spec.id}: {type(existing)!r}"
            )
            raise InternalStateCorruptionException(
                f"Unexpected routing state type for key {self.router_spec.id}: {type(existing)!r}"
            )
        return existing

    def _ordered_neurons_for_context(self, ctx: Context, neurons: Collection[Neuron]) -> list[Neuron]:
        ordered_neurons = sorted(neurons, key=lambda neuron: neuron.hotkey)
        random.Random(str(ctx.id)).shuffle(ordered_neurons)
        return ordered_neurons
