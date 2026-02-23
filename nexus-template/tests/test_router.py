# pyright: basic

from collections import Counter
from collections.abc import Callable, Sequence

import pytest
from fake_pylon_client import FakePylonClientProvider, build_neuron
from pylon_client.artanis.v1 import Neuron
from utils import CollectorActor, wait_until

from nexus.actors.neuron_router import (
    NoRoutableNeuronsException,
    RoundRobinNeuronRouter,
    Routed,
    miners_only,
    validators_only,
)
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Source
from nexus.core.runtime.context_store import ContextStore
from nexus.core.runtime.events import PipeToBus, SendEvent
from nexus.core.runtime.subnet_runtime import SubnetBuilder, SubnetRuntime
from nexus.utils.exceptions import NexusException


def _build_runtime(
    *,
    router: RoundRobinNeuronRouter[str],
    context_store: ContextStore | None = None,
    pipe_to_bus: PipeToBus | None = None,
) -> tuple[SubnetRuntime, CollectorActor[Routed[str]], CollectorActor[NexusException], Source[str]]:
    builder = SubnetBuilder(nodes=[router], context_store=context_store, pipe_to_bus=pipe_to_bus)
    routed_collector = CollectorActor[Routed[str]](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="router-collector",
    )
    error_collector = CollectorActor[NexusException](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="router-error-collector",
    )
    upstream_source = Source[str]("router-input-source")
    runtime = (
        builder
        .add_flows(
            Flow.from_connectable(upstream_source).then(router.input),
            Flow.from_connectable(router.routed).then(routed_collector.sink),
            Flow.from_connectable(router.error).then(error_collector.sink),
        )
        .add_actors(routed_collector, error_collector)
        .build()
    )
    return runtime, routed_collector, error_collector, upstream_source


def test_round_robin_router_cycles_through_neurons_per_context() -> None:
    pylon_provider = FakePylonClientProvider(
        neurons=[
            build_neuron(uid=1, hotkey="alpha", validator_permit=False),
            build_neuron(uid=2, hotkey="beta", validator_permit=True),
            build_neuron(uid=3, hotkey="gamma", validator_permit=False),
        ]
    )
    router = RoundRobinNeuronRouter[str](
        "router",
        netuid=42,
        pylon_client_provider=pylon_provider,
    )
    runtime, collector, error_collector, upstream_source = _build_runtime(router=router)

    with runtime.context_store.create_context() as context:
        ctx_id = context.id

    payloads = ["first", "second", "third", "fourth"]
    with runtime.running(shutdown_timeout_seconds=1.0):
        for payload in payloads:
            runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload=payload))

        wait_until(lambda: len(collector.received_events) == len(payloads))

    routed_payloads = [event.payload for event in collector.received_events]
    hotkeys = [payload.target.hotkey for payload in routed_payloads]
    assert set(hotkeys[:3]) == {"alpha", "beta", "gamma"}
    assert hotkeys[3] == hotkeys[0]
    assert [payload.input for payload in routed_payloads] == payloads
    assert len(error_collector.received_events) == 0


@pytest.mark.parametrize(
    ("neuron_filter", "expected_hotkeys", "expected_validator_permit"),
    [
        (miners_only, {"miner-a", "miner-b"}, False),
        (validators_only, {"validator-a", "validator-b"}, True),
    ],
)
def test_round_robin_router_filters_miners_or_validators(
    neuron_filter: Callable[[Sequence[Neuron]], Sequence[Neuron]],
    expected_hotkeys: set[str],
    expected_validator_permit: bool,
) -> None:
    pylon_provider = FakePylonClientProvider(
        neurons=[
            build_neuron(uid=1, hotkey="miner-a", validator_permit=False),
            build_neuron(uid=2, hotkey="validator-a", validator_permit=True),
            build_neuron(uid=3, hotkey="miner-b", validator_permit=False),
            build_neuron(uid=4, hotkey="validator-b", validator_permit=True),
        ]
    )
    router = RoundRobinNeuronRouter[str](
        "router-filtered",
        netuid=7,
        pylon_client_provider=pylon_provider,
        neuron_filter=neuron_filter,
    )
    runtime, collector, error_collector, upstream_source = _build_runtime(router=router)

    with runtime.context_store.create_context() as context:
        ctx_id = context.id

    payloads = ["a", "b", "c"]
    with runtime.running(shutdown_timeout_seconds=1.0):
        for payload in payloads:
            runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload=payload))

        wait_until(lambda: len(collector.received_events) == len(payloads))

    routed_payloads = [event.payload for event in collector.received_events]
    routed_hotkeys = [payload.target.hotkey for payload in routed_payloads]
    assert set(routed_hotkeys).issubset(expected_hotkeys)
    assert set(routed_hotkeys[:2]) == expected_hotkeys
    assert all(payload.target.validator_permit is expected_validator_permit for payload in routed_payloads)
    assert len(error_collector.received_events) == 0


def test_round_robin_router_accepts_custom_neuron_filter_function() -> None:
    pylon_provider = FakePylonClientProvider(
        neurons=[
            build_neuron(uid=1, hotkey="alpha", validator_permit=False),
            build_neuron(uid=2, hotkey="beta", validator_permit=True),
            build_neuron(uid=3, hotkey="gamma", validator_permit=False),
        ]
    )

    def only_beta(neurons: Sequence[Neuron]) -> Sequence[Neuron]:
        return [neuron for neuron in neurons if neuron.hotkey == "beta"]

    router = RoundRobinNeuronRouter[str](
        "router-custom-filter",
        netuid=8,
        pylon_client_provider=pylon_provider,
        neuron_filter=only_beta,
    )
    runtime, collector, error_collector, upstream_source = _build_runtime(router=router)

    with runtime.context_store.create_context() as context:
        ctx_id = context.id

    payloads = ["x", "y"]
    with runtime.running(shutdown_timeout_seconds=1.0):
        for payload in payloads:
            runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload=payload))

        wait_until(lambda: len(collector.received_events) == len(payloads))

    routed_hotkeys = [event.payload.target.hotkey for event in collector.received_events]
    assert routed_hotkeys == ["beta", "beta"]
    assert len(error_collector.received_events) == 0


def test_round_robin_router_balances_across_many_contexts() -> None:
    neurons = [
        build_neuron(uid=1, hotkey="alpha", validator_permit=False),
        build_neuron(uid=2, hotkey="beta", validator_permit=True),
        build_neuron(uid=3, hotkey="gamma", validator_permit=False),
        build_neuron(uid=4, hotkey="delta", validator_permit=True),
    ]
    pylon_provider = FakePylonClientProvider(neurons=neurons)
    router = RoundRobinNeuronRouter[str](
        "router-context-order",
        netuid=9,
        pylon_client_provider=pylon_provider,
    )
    runtime, collector, error_collector, upstream_source = _build_runtime(router=router)
    contexts_count = 1000
    payloads = [f"p-{idx}" for idx in range(contexts_count)]
    context_ids = []
    with runtime.running(shutdown_timeout_seconds=1.0):
        for payload in payloads:
            with runtime.context_store.create_context() as context:
                context_ids.append(context.id)
            runtime.pipe_to_bus.put(SendEvent(ctx_id=context_ids[-1], source=upstream_source, payload=payload))

        wait_until(lambda: len(collector.received_events) == len(payloads), timeout=3.0)

    routed_hotkeys = [str(event.payload.target.hotkey) for event in collector.received_events]
    counts = Counter(routed_hotkeys)
    expected_per_neuron = contexts_count / len(neurons)
    tolerance = expected_per_neuron * 0.30

    for neuron in neurons:
        hotkey = str(neuron.hotkey)
        assert hotkey in counts
        assert abs(counts[hotkey] - expected_per_neuron) <= tolerance
    assert len(error_collector.received_events) == 0


def test_round_robin_router_emits_error_when_no_neurons_in_pylon() -> None:
    pylon_provider = FakePylonClientProvider(neurons=[])
    router = RoundRobinNeuronRouter[str](
        "router-no-neurons",
        netuid=10,
        pylon_client_provider=pylon_provider,
    )
    runtime, routed_collector, error_collector, upstream_source = _build_runtime(router=router)

    with runtime.context_store.create_context() as context:
        ctx_id = context.id

    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload="payload"))
        wait_until(lambda: len(error_collector.received_events) == 1)

    assert len(routed_collector.received_events) == 0
    assert pylon_provider.netuid_calls == [10]

    error_event = error_collector.received_events[0]
    assert error_event.ctx_id == ctx_id
    assert isinstance(error_event.payload, NoRoutableNeuronsException)


def test_round_robin_router_emits_error_when_all_neurons_filtered_out() -> None:
    pylon_provider = FakePylonClientProvider(
        neurons=[
            build_neuron(uid=1, hotkey="validator-a", validator_permit=True),
            build_neuron(uid=2, hotkey="validator-b", validator_permit=True),
        ]
    )
    router = RoundRobinNeuronRouter[str](
        "router-all-filtered-out",
        netuid=11,
        pylon_client_provider=pylon_provider,
        neuron_filter=miners_only,
    )
    runtime, routed_collector, error_collector, upstream_source = _build_runtime(router=router)

    with runtime.context_store.create_context() as context:
        ctx_id = context.id

    with runtime.running(shutdown_timeout_seconds=1.0):
        runtime.pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=upstream_source, payload="payload"))
        wait_until(lambda: len(error_collector.received_events) == 1)

    assert len(routed_collector.received_events) == 0
    assert pylon_provider.netuid_calls == [11]

    error_event = error_collector.received_events[0]
    assert error_event.ctx_id == ctx_id
    assert isinstance(error_event.payload, NoRoutableNeuronsException)
