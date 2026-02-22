import queue
from datetime import timedelta
from typing import Generator
from unittest.mock import MagicMock, seal

import pytest
from pylon_client import v1 as pylon

from nexus.actors.chain_beat.epoch_beat import EpochBeatNode
from nexus.core.runtime.events import PipeToBus, StopActorEvent
from nexus.utils.types import BlockCount, BlockNumber

from utils import empty_context_store, dummy_epoch_beat


# Netuid 1 epochs for reference:
# -3 -> 357 (yes, it goes negative)
# 358 -> 718
# 719 -> 1079
# 1080 -> 1440
# 1441 -> 1801

@pytest.mark.parametrize("blocks, beats, delay", [
    pytest.param(
        [500, 800, 1200],
        [358, 719, 1080],  # Identify an epoch by its first block
        BlockCount(0),
        id="emits-epochs",
    ),
    pytest.param(
        [500, 501, 502, 800, 805, 810],
        [358, 719],
        BlockCount(0),
        id="emits-only-once"),
    pytest.param(
        [368, 718, 719, 720, 728],
        [358],  # With delay 10, should not emit epoch 719 until block 729
        BlockCount(10),
        id="respects-delay",
    ),
])
def test_epoch_beat(blocks: list[BlockNumber], beats: list[BlockNumber], delay: BlockCount, default_test_netuid):
    block_infos = [_dummy_block_info_response(block_number) for block_number in blocks]
    expected_beats = [dummy_epoch_beat(block_number, default_test_netuid) for block_number in beats]

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
    node = EpochBeatNode(
        "test",
        netuid=default_test_netuid,
        delay=delay,
        polling_interval=timedelta(seconds=0.01),
        pylon_client=client,
    )
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
