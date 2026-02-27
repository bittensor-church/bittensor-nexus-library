from datetime import timedelta
from itertools import chain, repeat
from unittest.mock import MagicMock, seal

import pytest
from pylon_client import artanis as pylon
from utils import CollectorActor, dummy_block_beat, wait_until

from nexus.actors.chain_beat.block_beat import BlockBeat, BlockBeatNode
from nexus.core.dsl.flow import Flow
from nexus.core.runtime.subnet_runtime import SubnetBuilder
from nexus.utils.types import BlockCount, BlockNumber


@pytest.mark.parametrize(
    "blocks, beats, nth",
    [
        pytest.param(
            [0, 1, 2],
            [0, 1, 2],
            BlockCount(1),
            id="all-consecutive-blocks-emitted",
        ),
        pytest.param(
            [0, 0, 0, 1, 1, 1],
            [0, 1],
            BlockCount(1),
            id="no-duplicates",
        ),
        pytest.param(
            [0, 1, 5, 7],
            [0, 1, 5, 7],
            BlockCount(1),
            id="no-hole-filling",
        ),
        pytest.param(
            [*range(15, 55)],
            [20, 30, 40, 50],
            BlockCount(10),
            id="every-nth-block",
        ),
    ],
)
def test_block_beat(blocks: list[BlockNumber], beats: list[BlockNumber], nth: BlockCount):
    block_infos = [_dummy_block_info_response(block_number) for block_number in blocks]
    expected_beats = [dummy_block_beat(block_number) for block_number in beats]

    # Repeat the last block forever so the producer doesn't crash on exhaustion; dedup prevents extra emissions
    client = MagicMock()
    client.open_access.get_latest_block_info.side_effect = chain(block_infos, repeat(block_infos[-1]))
    seal(client)

    beat = BlockBeatNode("test", pylon_client=client, polling_interval=timedelta(seconds=0.01), every_nth=nth)
    builder = SubnetBuilder(nodes=[beat])
    collector = CollectorActor[BlockBeat](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
    )

    runtime = builder.add_flows(Flow.from_connectable(beat.source).then(collector.sink)).add_actors(collector).build()

    with runtime.running(shutdown_timeout_seconds=1.0):
        wait_until(lambda: len(collector.received_events) >= len(expected_beats))

    assert [event.payload for event in collector.received_events] == expected_beats


def _dummy_block_info_response(block_number: BlockNumber) -> pylon.GetLatestBlockInfoResponse:
    return pylon.v1.GetLatestBlockInfoResponse(
        number=pylon.BlockNumber(block_number),
        timestamp=pylon.Timestamp(block_number * 1000),
        hash=pylon.BlockHash(f"0x{block_number:064x}"),
    )
