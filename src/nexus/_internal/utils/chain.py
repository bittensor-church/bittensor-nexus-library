from nexus._internal.utils.types import BlockNumber, Epoch, NetUid, Tempo

DEFAULT_TEMPO = Tempo(360)  # This is actually a subnet hyperparam. It's rare for it to be changed, but possible.


def get_epoch_containing_block(block: BlockNumber, netuid: NetUid, tempo: Tempo = DEFAULT_TEMPO) -> Epoch:
    """
    Reimplementing the logic from subtensor's Rust function:
        pub fn blocks_until_next_epoch(netuid: u16, tempo: u16, block_number: u64) -> u64
    See https://github.com/opentensor/subtensor.

    See also: https://github.com/opentensor/bittensor/pull/2168/commits/9e8745447394669c03d9445373920f251630b6b8

    Raises:
        ValueError: If tempo is not positive (tempo <= 0).

    """
    if tempo <= 0:
        raise ValueError("tempo must be positive")

    interval = tempo + 1
    next_epoch = block + tempo - (block + netuid + 1) % interval

    if next_epoch == block:
        prev_epoch = next_epoch
        next_epoch = prev_epoch + interval
    else:
        prev_epoch = next_epoch - interval

    return Epoch(BlockNumber(prev_epoch), BlockNumber(next_epoch - 1))
