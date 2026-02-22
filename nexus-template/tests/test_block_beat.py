import queue
from datetime import timedelta
from typing import Generator
from unittest.mock import MagicMock, seal

import pytest
from pylon_client import v1 as pylon

from nexus.actors.chain_beat.block_beat import BlockBeatNode
from nexus.core.runtime.events import PipeToBus, StopActorEvent
from nexus.utils.types import BlockNumber, BlockCount
from utils import empty_context_store, dummy_block_beat


@pytest.mark.parametrize("blocks, beats, nth", [
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
])
def test_block_beat(blocks: list[BlockNumber], beats: list[BlockNumber], nth: BlockCount):
    block_infos = [_dummy_block_info_response(block_number) for block_number in blocks]
    expected_beats = [dummy_block_beat(block_number) for block_number in beats]

    def get_latest() -> Generator[pylon.GetLatestBlockInfoResponse]:
        for idx, block in enumerate(block_infos):
            if idx == len(blocks) - 1:
                actor.pipe_from_bus.put(StopActorEvent())
            yield block
        raise Exception("Consumed more blocks than expected!")

    client = MagicMock()
    client.open_access.get_latest_block_info.side_effect = get_latest()
    seal(client)

    pipe_to_bus: PipeToBus = queue.Queue()
    node = BlockBeatNode("test", pylon_client=client, polling_interval=timedelta(seconds=0.01), every_nth=nth)
    actor = node.build_actor(pipe_to_bus=pipe_to_bus, context_store=empty_context_store())

    actor_thread = actor.run_loop()
    actor_thread.join(timeout=1)

    emitted_beats = []
    while not pipe_to_bus.empty():
        emitted_beats.append(pipe_to_bus.get_nowait().payload)

    assert emitted_beats == expected_beats
    assert not actor_thread.is_alive()


def _dummy_block_info_response(block_number: BlockNumber) -> pylon.GetLatestBlockInfoResponse:
    return pylon.GetLatestBlockInfoResponse(
        number=pylon.BlockNumber(block_number),
        timestamp=pylon.Timestamp(block_number * 1000),
        hash=pylon.BlockHash(f"0x{block_number:064x}"),
    )