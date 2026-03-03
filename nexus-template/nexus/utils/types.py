"""
For convenience, pylon types are reused to avoid "casting" stuff like block numbers on nexus-pylon interface.
There are a ton more available - check them out before creating new types here.
"""

from typing import NamedTuple, NewType

from pylon_client.artanis import BlockHash, BlockNumber, Hotkey, NetUid, Port, Tempo, Timestamp, Weight

BlockCount = NewType("BlockCount", int)


class Epoch(NamedTuple):
    first_block: BlockNumber
    last_block: BlockNumber


__all__ = [
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
