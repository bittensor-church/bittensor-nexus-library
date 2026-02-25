# pyright: basic
from collections.abc import Mapping
from unittest.mock import create_autospec, seal

import pytest
from pylon_client.artanis import PylonResponseException
from utils import CollectorActor, wait_until

from nexus.actors import PylonClientProvider
from nexus.actors.chain_beat.epoch_beat import EpochBeat
from nexus.actors.pylon_client_provider import IdentityPylonApiLike, SyncPylonClientLike
from nexus.actors.weight_setter import (
    WeighingFunc,
    WeightSetterNode,
    WeightSettingSuccess,
)
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Source
from nexus.core.runtime.events import SendEvent
from nexus.core.runtime.subnet_runtime import SubnetBuilder
from nexus.utils.chain import get_epoch_containing_block
from nexus.utils.exceptions import NexusException, WeightSettingException
from nexus.utils.types import BlockNumber, Hotkey, NetUid, Weight

NETUID = NetUid(1)
EPOCH = get_epoch_containing_block(BlockNumber(500), netuid=NETUID)
WEIGHTS = {Hotkey("hk1"): Weight(0.5), Hotkey("hk2"): Weight(1.0)}


def _raise(exc: Exception) -> Mapping[Hotkey, Weight]:
    """
    A lambda cannot raise, hence this helper.
    """
    raise exc


@pytest.fixture
def mock_pylon_client():
    client = create_autospec(spec=SyncPylonClientLike, instance=True)
    client.identity = create_autospec(spec=IdentityPylonApiLike, instance=True)
    seal(client)
    return client


def _build_and_run(
    *,
    weighing_func: WeighingFunc,
    pylon_client: SyncPylonClientLike,
) -> tuple[list[WeightSettingSuccess], list[NexusException]]:
    provider = create_autospec(spec=PylonClientProvider, instance=True)
    provider.get_client.return_value = pylon_client
    seal(provider)

    trigger = Source[EpochBeat]("test-trigger")
    node = WeightSetterNode("test-weight-setter", weighing_func=weighing_func, pylon_client_provider=provider)

    builder = SubnetBuilder(nodes=[node])
    ok_collector = CollectorActor[WeightSettingSuccess](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="ok-collector",
    )
    error_collector = CollectorActor[NexusException](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="error-collector",
    )

    flow = Flow.from_connectable(trigger).then(node).then(ok=ok_collector.sink, error=error_collector.sink)

    runtime = builder.add_flows(flow).add_actors(ok_collector, error_collector).build()

    with runtime.running(shutdown_timeout_seconds=1.0):
        with builder.context_store.create_context() as ctx:
            pass
        builder.pipe_to_bus.put(SendEvent(ctx_id=ctx.id, source=trigger, payload=EpochBeat(epoch=EPOCH)))
        wait_until(lambda: len(ok_collector.received_events) + len(error_collector.received_events) >= 1)

    return (
        [e.payload for e in ok_collector.received_events],
        [e.payload for e in error_collector.received_events],
    )


def test_happy_path_sets_weights_and_emits_success(mock_pylon_client):
    ok, errors = _build_and_run(
        weighing_func=lambda bundle: WEIGHTS,
        pylon_client=mock_pylon_client,
    )
    assert errors == []
    assert ok == [WeightSettingSuccess()]
    mock_pylon_client.identity.put_weights.assert_called_once_with(WEIGHTS)


def test_weighing_failure_emits_error(mock_pylon_client):
    ok, errors = _build_and_run(
        weighing_func=lambda bundle: _raise(Exception("calculation exploded")),
        pylon_client=mock_pylon_client,
    )
    assert [type(e) for e in errors] == [WeightSettingException]
    assert ok == []
    mock_pylon_client.identity.put_weights.assert_not_called()


def test_pylon_failure_emits_error(mock_pylon_client):
    mock_pylon_client.identity.put_weights.side_effect = PylonResponseException("pylon is not cooperating")
    ok, errors = _build_and_run(
        weighing_func=lambda bundle: WEIGHTS,
        pylon_client=mock_pylon_client,
    )
    assert [type(e) for e in errors] == [WeightSettingException]
    assert ok == []
