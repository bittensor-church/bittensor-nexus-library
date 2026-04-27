# pyright: basic
from nexus.utils.types import BlockNumber, Epoch


def test_epoch_previous_returns_immediately_preceding_epoch() -> None:
    current_epoch = Epoch(first_block=BlockNumber(100), last_block=BlockNumber(199))

    assert current_epoch.previous() == Epoch(
        first_block=BlockNumber(0),
        last_block=BlockNumber(99),
    )
