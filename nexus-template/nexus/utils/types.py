from typing import NewType, NamedTuple

BlockNumber = NewType("BlockNumber", int)
BlockCount = NewType("BlockCount", int)
BlockTimestamp = NewType("BlockTimestamp", int)
BlockHash = NewType("BlockHash", str)
Tempo = NewType("Tempo", int)
SubnetId = NewType("SubnetId", int)


class Epoch(NamedTuple):
    start: BlockNumber
    end: BlockNumber
