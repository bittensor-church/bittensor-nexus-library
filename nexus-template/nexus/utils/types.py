"""
For convenience, pylon types are reused to avoid "casting" stuff like block numbers on nexus-pylon interface.
There are a ton more available - check them out before creating new types here.
"""

from typing import NamedTuple, NewType

from pylon_client.artanis import BlockHash, BlockNumber, Hotkey, NetUid, Tempo, Timestamp, Weight

BlockCount = NewType("BlockCount", int)


class Epoch(NamedTuple):
    """Represents an epoch as a range of block numbers,
    inclusive of the first and last block.
    """
    first_block: BlockNumber
    last_block: BlockNumber

    def contains(self, block_number: BlockNumber) -> bool:
        """Checks if the given block number is within this epoch."""
        return self.first_block <= block_number <= self.last_block


__all__ = [
    "BlockCount",
    "BlockHash",
    "BlockNumber",
    "Epoch",
    "Hotkey",
    "NetUid",
    "Tempo",
    "Timestamp",
    "Weight",
]
