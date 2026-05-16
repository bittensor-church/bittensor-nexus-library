# pyright: basic

from types import SimpleNamespace

import pytest
from pylon_client import artanis
from utils import CollectorActor, MockPylonClientProvider, dummy_block_beat, wait_until

from nexus.v1 import (
    BlockBeat,
    BlockCount,
    BlockNumber,
    Epoch,
    Flow,
    NetUid,
    SendEvent,
    SetWeightsBeat,
    SetWeightsBeatNode,
    Source,
    SubnetBuilder,
    WeightSettingSuccess,
    get_epoch_containing_block,
)

# Netuid 1 epochs (tempo=360):
# -3 -> 357
# 358 -> 718
# 719 -> 1079


def _weights_status_response(*, weights_set: bool) -> SimpleNamespace:
    # GetWeightsStatusResponse isn't shipped in the pylon_client version under test;
    # SimpleNamespace.weights_set is the only attribute SetWeightsBeatActor reads.
    return SimpleNamespace(weights_set=weights_set)


def _send_block_beat(*, pipe_to_bus, ctx_id, source: Source[BlockBeat], block_number: int) -> None:
    pipe_to_bus.put(
        SendEvent(
            ctx_id=ctx_id,
            source=source,
            payload=dummy_block_beat(block_number),
        )
    )


def _send_weights_set(
    *,
    pipe_to_bus,
    ctx_id,
    source: Source[WeightSettingSuccess],
    epoch: Epoch,
) -> None:
    pipe_to_bus.put(
        SendEvent(
            ctx_id=ctx_id,
            source=source,
            payload=WeightSettingSuccess(epoch=epoch),
        )
    )


def _build_runtime(
    *,
    node: SetWeightsBeatNode,
) -> tuple[SubnetBuilder, CollectorActor[SetWeightsBeat], Source[BlockBeat], Source[WeightSettingSuccess]]:
    block_beat_trigger = Source[BlockBeat]("test-block-beat-trigger")
    weights_set_trigger = Source[WeightSettingSuccess]("test-weights-set-trigger")

    builder = SubnetBuilder(nodes=[node])
    collector = CollectorActor[SetWeightsBeat](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
    )

    flow_in = Flow.from_connectable(block_beat_trigger).then(node.block_beat)
    flow_ws = Flow.from_connectable(weights_set_trigger).then(node.weights_set)
    flow_out = Flow.from_connectable(node.source).then(collector.sink)

    builder.add_flows(flow_in, flow_ws, flow_out).add_actors(collector)
    return builder, collector, block_beat_trigger, weights_set_trigger


def test_emits_when_all_conditions_met(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.identity.get_weights_status.return_value = _weights_status_response(weights_set=False)

    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger, _ = _build_runtime(node=node)
    runtime = builder.build()

    epoch_500 = get_epoch_containing_block(BlockNumber(500), netuid=default_test_netuid)

    with runtime.running(shutdown_timeout_seconds=1.0):
        with builder.context_store.create_context() as ctx:
            pass
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=500)
        wait_until(lambda: len(collector.received_events) >= 1)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_500, block_number=BlockNumber(500)),
    ]


def test_skips_when_too_early_in_epoch(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.identity.get_weights_status.return_value = _weights_status_response(weights_set=False)

    # Epoch 358..718; offset 20 means first eligible block is 378. 360 must be skipped, 380 must emit.
    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(20),
        attempts_cooldown=BlockCount(1),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger, _ = _build_runtime(node=node)
    runtime = builder.build()

    epoch_380 = get_epoch_containing_block(BlockNumber(380), netuid=default_test_netuid)

    with runtime.running(shutdown_timeout_seconds=1.0):
        with builder.context_store.create_context() as ctx:
            pass
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=360)
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=380)
        wait_until(lambda: len(collector.received_events) >= 1)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_380, block_number=BlockNumber(380)),
    ]


def test_skips_when_weights_already_set_in_epoch(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.identity.get_weights_status.return_value = _weights_status_response(weights_set=False)

    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        attempts_cooldown=BlockCount(1),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger, weights_set_trigger = _build_runtime(node=node)
    runtime = builder.build()

    epoch_500 = get_epoch_containing_block(BlockNumber(500), netuid=default_test_netuid)

    with runtime.running(shutdown_timeout_seconds=1.0):
        with builder.context_store.create_context() as ctx:
            pass
        # First emit, then signal success, then another block beat in the same epoch — must NOT emit again.
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=500)
        wait_until(lambda: len(collector.received_events) >= 1)
        _send_weights_set(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=weights_set_trigger, epoch=epoch_500)
        # Give the actor a chance to process the weights_set event before the next block beat.
        # The next block beat should be ignored due to the in-epoch success flag.
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=600)
        # Wait briefly to ensure no additional emissions happen.
        with pytest.raises(AssertionError):
            wait_until(lambda: len(collector.received_events) >= 2, timeout=0.5)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_500, block_number=BlockNumber(500)),
    ]


def test_resets_on_new_epoch(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.identity.get_weights_status.return_value = _weights_status_response(weights_set=False)

    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        attempts_cooldown=BlockCount(1),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger, weights_set_trigger = _build_runtime(node=node)
    runtime = builder.build()

    epoch_500 = get_epoch_containing_block(BlockNumber(500), netuid=default_test_netuid)
    epoch_719 = get_epoch_containing_block(BlockNumber(719), netuid=default_test_netuid)
    assert epoch_500 != epoch_719

    with runtime.running(shutdown_timeout_seconds=1.0):
        with builder.context_store.create_context() as ctx:
            pass
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=500)
        wait_until(lambda: len(collector.received_events) >= 1)
        _send_weights_set(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=weights_set_trigger, epoch=epoch_500)
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=719)
        wait_until(lambda: len(collector.received_events) >= 2)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_500, block_number=BlockNumber(500)),
        SetWeightsBeat(epoch=epoch_719, block_number=BlockNumber(719)),
    ]


def test_respects_attempts_cooldown(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.identity.get_weights_status.return_value = _weights_status_response(weights_set=False)

    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        attempts_cooldown=BlockCount(4),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger, _ = _build_runtime(node=node)
    runtime = builder.build()

    epoch_500 = get_epoch_containing_block(BlockNumber(500), netuid=default_test_netuid)

    with runtime.running(shutdown_timeout_seconds=1.0):
        with builder.context_store.create_context() as ctx:
            pass
        # 500 -> emit; 502 -> skip (cooldown=4, gap=2); 504 -> emit (gap=4).
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=500)
        wait_until(lambda: len(collector.received_events) >= 1)
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=502)
        with pytest.raises(AssertionError):
            wait_until(lambda: len(collector.received_events) >= 2, timeout=0.3)
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=504)
        wait_until(lambda: len(collector.received_events) >= 2)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_500, block_number=BlockNumber(500)),
        SetWeightsBeat(epoch=epoch_500, block_number=BlockNumber(504)),
    ]


def test_skips_when_pylon_says_weights_set(default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.identity.get_weights_status.return_value = _weights_status_response(weights_set=True)

    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        attempts_cooldown=BlockCount(1),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger, _ = _build_runtime(node=node)
    runtime = builder.build()

    with runtime.running(shutdown_timeout_seconds=1.0):
        with builder.context_store.create_context() as ctx:
            pass
        _send_block_beat(pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=500)
        with pytest.raises(AssertionError):
            wait_until(lambda: len(collector.received_events) >= 1, timeout=0.5)

    assert collector.received_events == []


def test_pylon_transient_failure_is_logged_and_ignored(caplog: pytest.LogCaptureFixture, default_test_netuid: NetUid):
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.identity.get_weights_status.side_effect = [
            artanis.PylonRequestException("temporarily unavailable"),
            _weights_status_response(weights_set=False),
        ]

    node = SetWeightsBeatNode(
        "test",
        netuid=default_test_netuid,
        epoch_start_offset=BlockCount(0),
        attempts_cooldown=BlockCount(1),
        pylon_client_provider=provider,
    )
    builder, collector, block_beat_trigger, _ = _build_runtime(node=node)
    runtime = builder.build()

    epoch_500 = get_epoch_containing_block(BlockNumber(500), netuid=default_test_netuid)

    with caplog.at_level("WARNING", logger="nexus._internal.actors.chain_beat.set_weights_beat"):
        with runtime.running(shutdown_timeout_seconds=1.0):
            with builder.context_store.create_context() as ctx:
                pass
            _send_block_beat(
                pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=500
            )
            # First call raised; no emission yet.
            with pytest.raises(AssertionError):
                wait_until(lambda: len(collector.received_events) >= 1, timeout=0.3)
            _send_block_beat(
                pipe_to_bus=builder.pipe_to_bus, ctx_id=ctx.id, source=block_beat_trigger, block_number=501
            )
            wait_until(lambda: len(collector.received_events) >= 1)

    assert [event.payload for event in collector.received_events] == [
        SetWeightsBeat(epoch=epoch_500, block_number=BlockNumber(501)),
    ]
    assert any("Transient Pylon poll failure" in record.message for record in caplog.records)
