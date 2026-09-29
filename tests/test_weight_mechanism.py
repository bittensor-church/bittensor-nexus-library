from collections.abc import Mapping
from typing import override
from unittest.mock import call

import httpx
import pytest
from pylon_client.artanis import Config, IdentityName, PylonAuthToken, PylonClient
from pylon_client.artanis.unstable import GetWeightsStatusResponse
from utils import InMemoryTestTaskResultStoreProvider, MockPylonClientProvider, dummy_block_beat

from nexus.v1 import (
    BlockCount,
    BlockNumber,
    Hotkey,
    MechanismId,
    NetUid,
    PylonClientProvider,
    ReceiveEvent,
    SetWeightsBeat,
    SetWeightsBeatActor,
    SetWeightsBeatNode,
    SubnetBuilder,
    Weight,
    WeightsCalculationBundle,
    WeightSetterNode,
    get_epoch_containing_block,
)


def _weigh(_: WeightsCalculationBundle) -> Mapping[Hotkey, Weight]:
    return {Hotkey("miner"): Weight(1.0)}


def _poll(builder: SubnetBuilder, actor: SetWeightsBeatActor, block: int) -> bool:
    with builder.context_store.create_context() as ctx:
        return bool(
            actor.handlers()[actor.spec.block_beat](
                ctx,
                ReceiveEvent(ctx_id=ctx.id, target=actor.spec.block_beat, payload=dummy_block_beat(block)),
            )
        )


@pytest.mark.parametrize("mechanism_id", [MechanismId(0), MechanismId(1)])
def test_poll_forwards_selected_mechanism(mechanism_id: MechanismId) -> None:
    provider = MockPylonClientProvider()
    with provider.prepare_mock_client() as client:
        client.unstable.identity.get_weights_status.return_value = GetWeightsStatusResponse(weights_submitted=False)
    node = SetWeightsBeatNode(
        "beat",
        netuid=NetUid(12),
        mechanism_id=mechanism_id,
        epoch_start_offset=BlockCount(0),
        pylon_client_provider=provider,
    )
    builder = SubnetBuilder(nodes=[node])
    actor = node.build_actor(pipe_to_bus=builder.pipe_to_bus, context_store=builder.context_store)
    assert _poll(builder, actor, 500)
    client.unstable.identity.get_weights_status.assert_called_once_with(
        block_number=BlockNumber(500), mechanism_id=mechanism_id
    )


def test_cached_mechanism_zero_submission_does_not_suppress_mechanism_one() -> None:
    provider = MockPylonClientProvider()

    def status(block_number: BlockNumber, mechanism_id: MechanismId = MechanismId(0)) -> GetWeightsStatusResponse:  # noqa: B008
        return GetWeightsStatusResponse(weights_submitted=mechanism_id == 0)

    with provider.prepare_mock_client() as client:
        client.unstable.identity.get_weights_status.side_effect = status
    nodes = [
        SetWeightsBeatNode(
            f"beat-{mechanism_id}",
            netuid=NetUid(12),
            mechanism_id=MechanismId(mechanism_id),
            epoch_start_offset=BlockCount(0),
            pylon_client_provider=provider,
        )
        for mechanism_id in (0, 1)
    ]
    builder = SubnetBuilder(nodes=nodes)
    zero, one = [
        node.build_actor(pipe_to_bus=builder.pipe_to_bus, context_store=builder.context_store) for node in nodes
    ]
    assert not _poll(builder, zero, 500)
    assert not _poll(builder, zero, 501)  # Cached success; no second Pylon read.
    assert _poll(builder, one, 501)
    assert client.unstable.identity.get_weights_status.call_args_list == [
        call(block_number=BlockNumber(500), mechanism_id=MechanismId(0)),
        call(block_number=BlockNumber(501), mechanism_id=MechanismId(1)),
    ]


def test_no_arbitrary_upper_bound() -> None:
    mechanism_id = MechanismId(256)
    assert (
        WeightSetterNode(
            "setter",
            mechanism_id=mechanism_id,
            weighing_func=_weigh,
        ).mechanism_id
        == mechanism_id
    )
    assert (
        SetWeightsBeatNode(
            "beat",
            netuid=NetUid(12),
            mechanism_id=mechanism_id,
            epoch_start_offset=BlockCount(0),
        ).mechanism_id
        == mechanism_id
    )


class _HttpPylonProvider(PylonClientProvider):
    @override
    def get_client(self) -> PylonClient:
        return PylonClient(
            Config(
                address="http://pylon.test",
                identity_name=IdentityName("validator"),
                identity_token=PylonAuthToken("test"),
            )
        )


@pytest.mark.parametrize("mechanism_id", [MechanismId(0), MechanismId(1)])
def test_nodes_use_pinned_client_mechanism_routes(monkeypatch: pytest.MonkeyPatch, mechanism_id: MechanismId) -> None:
    """Exercise real client serialization with HTTP intercepted before any network I/O."""
    requests: list[tuple[str, str]] = []

    def send(_client: httpx.Client, request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path))
        if request.url.path.endswith("/identities"):
            return httpx.Response(200, json={"identities": {"validator": 12}}, request=request)
        if request.url.path.endswith("/weights/status"):
            return httpx.Response(200, json={"weights_submitted": False}, request=request)
        return httpx.Response(200, json={}, request=request)

    monkeypatch.setattr(httpx.Client, "send", send)
    provider = _HttpPylonProvider()
    trigger = SetWeightsBeatNode(
        "trigger",
        netuid=NetUid(12),
        mechanism_id=mechanism_id,
        epoch_start_offset=BlockCount(0),
        pylon_client_provider=provider,
    )
    setter = WeightSetterNode(
        "setter",
        weighing_func=_weigh,
        mechanism_id=mechanism_id,
        pylon_client_provider=provider,
        task_result_store_provider=InMemoryTestTaskResultStoreProvider[str, str, str](),
    )
    builder = SubnetBuilder(nodes=[trigger, setter])
    poller = trigger.build_actor(pipe_to_bus=builder.pipe_to_bus, context_store=builder.context_store)
    writer = setter.build_actor(pipe_to_bus=builder.pipe_to_bus, context_store=builder.context_store)
    assert _poll(builder, poller, 500)
    with builder.context_store.create_context() as ctx:
        writer.handlers()[setter.sink](
            ctx,
            ReceiveEvent(
                ctx_id=ctx.id,
                target=setter.sink,
                payload=SetWeightsBeat(
                    epoch=get_epoch_containing_block(BlockNumber(500), netuid=NetUid(12)), block_number=BlockNumber(500)
                ),
            ),
        )
    assert [(method, path) for method, path in requests if not path.endswith("/identities")] == [
        ("GET", f"/api/_unstable/identity/validator/subnet/12/mechanism/{mechanism_id}/block/500/weights/status"),
        ("PUT", f"/api/_unstable/identity/validator/subnet/12/mechanism/{mechanism_id}/weights"),
    ]
