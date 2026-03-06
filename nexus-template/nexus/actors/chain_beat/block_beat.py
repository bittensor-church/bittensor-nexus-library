from __future__ import annotations

import logging
import time
from collections.abc import Generator
from dataclasses import dataclass
from datetime import timedelta
from threading import Event
from typing import override

from pylon_client.artanis import BasePylonException

from nexus.actors.pylon_client_provider import DEFAULT_PYLON_CLIENT_PROVIDER, PylonClientProvider
from nexus.core.dsl.nodes import Producer
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import ProducerActor
from nexus.core.runtime.context_store import ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.logging_utils import get_logger
from nexus.utils.types import BlockCount, BlockHash, BlockNumber, Timestamp

logger: logging.Logger = get_logger(__name__)


@dataclass(frozen=True)
class BlockBeat:
    block_number: BlockNumber
    block_timestamp: Timestamp
    block_hash: BlockHash


class BlockBeatNode(Producer[BlockBeat], ActorBuilder):
    """
    Uses pylon, polling it in a loop to retrieve the latest block info.
    Emits a message - the current block info - whenever it changes.
    Guarantees:
     - Emitted block number is real as received from the chain (so, not time-predicted)
    Keep in mind:
     - There may be gaps. If we lose connection to pylon and regain it after a while, there's no backfilling.
     - The beat will not happen at a stable 12-second pace, there will be some seconds of jitter and delay. The beat
       object contains the block timestamp if you need to know it.
    """

    every_nth: BlockCount
    polling_interval: timedelta
    pylon_client_provider: PylonClientProvider

    def __init__(
        self,
        _id: str,
        *,
        every_nth: BlockCount = BlockCount(1),  # noqa: B008
        polling_interval: timedelta = timedelta(seconds=1),
        pylon_client_provider: PylonClientProvider | None = None,
    ) -> None:
        """
        Args:
            _id: The node ID / name
            every_nth: Emit only every n-th block
            polling_interval: How often to poll for the latest block
            pylon_client_provider: Provider for pylon client instances
        """
        super().__init__(_id)
        self.every_nth = every_nth
        self.polling_interval = polling_interval
        self.pylon_client_provider = pylon_client_provider or DEFAULT_PYLON_CLIENT_PROVIDER

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> BlockBeatActor:
        return BlockBeatActor(beat_spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class BlockBeatActor(ProducerActor[BlockBeat]):
    beat_spec: BlockBeatNode
    _stop_event: Event

    def __init__(self, beat_spec: BlockBeatNode, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec=beat_spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.beat_spec = beat_spec
        self._stop_event = Event()

    @override
    def on_stop(self) -> None:
        self._stop_event.set()

    @override
    def _produce(self) -> Generator[BlockBeat]:
        last_emitted: BlockNumber | None = None
        interval_seconds = self.beat_spec.polling_interval.total_seconds()
        pylon = self.beat_spec.pylon_client_provider.get_client()
        every_nth = self.beat_spec.every_nth

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
                block_number = BlockNumber(response.number)

                if block_number != last_emitted and block_number % every_nth == 0:
                    logger.info(f"New block: {block_number}")
                    last_emitted = block_number
                    yield BlockBeat(
                        block_number=block_number,
                        block_timestamp=Timestamp(response.timestamp),
                        block_hash=BlockHash(response.hash),
                    )

            remaining = interval_seconds - (time.monotonic() - poll_start)
            if remaining > 0:
                self._stop_event.wait(timeout=remaining)
