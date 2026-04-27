"""
For convenience, pylon types are reused to avoid "casting" stuff like block numbers on nexus-pylon interface.
There are a ton more available - check them out before creating new types here.
"""

from typing import NamedTuple, NewType

from pylon_client.artanis import BlockHash, BlockNumber, Hotkey, NetUid, Port, Tempo, Timestamp, Weight
from pylon_client.artanis.v1 import AxonProtocol

BlockCount = NewType("BlockCount", int)


class Epoch(NamedTuple):
    """
    Represents an epoch as a range of block numbers,
    inclusive of the first and last block.
    """

    first_block: BlockNumber
    last_block: BlockNumber

    def contains(self, block_number: BlockNumber) -> bool:
        """Checks if the given block number is within this epoch."""
        return self.first_block <= block_number <= self.last_block

    def previous(self) -> Epoch:
        """
        Returns the immediately preceding epoch with the same block span.

        Raises:
            ValueError: If this epoch cannot be shifted backward without block underflow.

        """
        epoch_span = int(self.last_block) - int(self.first_block) + 1
        if epoch_span <= 0:
            raise ValueError(f"Epoch span must be positive, got {epoch_span}")

        previous_first_block = int(self.first_block) - epoch_span
        previous_last_block = int(self.last_block) - epoch_span
        if previous_first_block < 0 or previous_last_block < 0:
            raise ValueError(f"Cannot derive previous epoch from {self}: block underflow")

        return Epoch(
            first_block=BlockNumber(previous_first_block),
            last_block=BlockNumber(previous_last_block),
        )


__all__ = [
    "AxonProtocol",
    "BlockCount",
    "BlockHash",
    "BlockNumber",
    "Epoch",
    "Hotkey",
    "NetUid",
    "Port",
    "Tempo",
    "Timestamp",
    "Weight",
]
