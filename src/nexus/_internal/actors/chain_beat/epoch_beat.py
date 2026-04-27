from __future__ import annotations

import logging
import time
from collections.abc import Generator
from dataclasses import dataclass
from datetime import timedelta
from threading import Event
from typing import override

from pylon_client.artanis import BasePylonException

from nexus._internal.actors.pylon_client_provider import DEFAULT_PYLON_CLIENT_PROVIDER, PylonClientProvider
from nexus._internal.core.dsl.nodes import Producer
from nexus._internal.core.runtime.actor import ActorBuilder
from nexus._internal.core.runtime.actor_patterns import ProducerActor
from nexus._internal.core.runtime.context_store import ContextStore
from nexus._internal.core.runtime.events import PipeToBus
from nexus._internal.logging_utils import get_logger
from nexus._internal.utils.chain import get_epoch_containing_block
from nexus._internal.utils.types import BlockCount, BlockNumber, Epoch, NetUid

logger: logging.Logger = get_logger(__name__)


@dataclass(frozen=True)
class EpochBeat:
    epoch: Epoch


class EpochBeatNode(Producer[EpochBeat], ActorBuilder):
    """
    Emits a beat whenever the epoch changes for a given subnet.
    Epoch boundaries are derived from block numbers, so the same gaps and jitter
    from BlockBeatNode apply here.
    The optional delay parameter delays the beat after the actual epoch change,
    useful for waiting for finalization, staggering epoch-scheduled events, etc.
    The delay is specified in the number of blocks.

    sink sink: unused (lifecycle/control)
    source source: EpochBeat on each new epoch
    """

    netuid: NetUid
    delay_blocks: BlockCount
    polling_interval: timedelta
    pylon_client_provider: PylonClientProvider

    def __init__(
        self,
        _id: str,
        *,
        netuid: NetUid,
        delay: BlockCount = BlockCount(0),  # noqa: B008
        polling_interval: timedelta = timedelta(seconds=1),
        pylon_client_provider: PylonClientProvider | None = None,
    ) -> None:
        """
        Args:
            _id: The node ID / name
            netuid: The subnet number for which to monitor the epoch
            delay: How many blocks after the start of an epoch to emit the beat
            polling_interval: How often to poll for the latest block number
            pylon_client_provider: Provider for pylon client instances

        """
        super().__init__(_id)
        self.netuid = netuid
        self.delay_blocks = delay
        self.polling_interval = polling_interval
        self.pylon_client_provider = pylon_client_provider or DEFAULT_PYLON_CLIENT_PROVIDER

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> EpochBeatActor:
        return EpochBeatActor(beat_spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class EpochBeatActor(ProducerActor[EpochBeat]):
    beat_spec: EpochBeatNode
    _stop_event: Event

    def __init__(self, beat_spec: EpochBeatNode, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec=beat_spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.beat_spec = beat_spec
        self._stop_event = Event()

    @override
    def on_stop(self) -> None:
        self._stop_event.set()

    @override
    def _produce(self) -> Generator[EpochBeat]:
        last_emitted: Epoch | None = None
        interval_seconds = self.beat_spec.polling_interval.total_seconds()
        delay_blocks = self.beat_spec.delay_blocks
        netuid = self.beat_spec.netuid
        pylon = self.beat_spec.pylon_client_provider.get_client()

        while not self._stop_event.is_set():
            poll_start = time.monotonic()

            try:
                # 1. Retry on BasePylonException - request/timeout/response failures may be transient.
                # 2. Bubble up all non-Pylon exceptions - ProducerActor will handle those as unexpected failures.
                with pylon:
                    response = pylon.open_access.get_latest_block_info()

            except BasePylonException as exc:
                logger.warning(
                    "Transient Pylon poll failure; will retry. error_type=%s error=%s",
                    type(exc).__name__,
                    exc,
                )

            else:
                current_block_number = BlockNumber(response.number)
                epoch = get_epoch_containing_block(
                    block=BlockNumber(current_block_number - delay_blocks),
                    netuid=netuid,
                )

                if epoch != last_emitted:
                    logger.info(f"New epoch: {epoch}")
                    last_emitted = epoch
                    yield EpochBeat(epoch=epoch)

            remaining = interval_seconds - (time.monotonic() - poll_start)
            if remaining > 0:
                self._stop_event.wait(timeout=remaining)
