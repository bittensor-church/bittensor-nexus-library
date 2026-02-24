from __future__ import annotations
from dataclasses import dataclass
import logging
import time
from datetime import timedelta
from threading import Event
from typing import override, Generator

from pylon_client.v1 import PylonClient

from nexus.core.dsl.nodes import Source, Node, NodeSources, NodeSinks, SourceName
from nexus.core.runtime.actor import ActorBuilder
from nexus.core.runtime.actor_patterns import ProducerActor
from nexus.core.runtime.context_store import ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.logging_utils import get_logger
from nexus.utils.types import BlockNumber, BlockTimestamp, BlockHash, BlockCount

logger: logging.Logger = get_logger(__name__)


@dataclass
class BlockBeat:
    block_number: BlockNumber
    block_timestamp: BlockTimestamp
    block_hash: BlockHash


class BlockBeatNode(Node, ActorBuilder):
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

    def __init__(
        self,
        _id: str,
        *,
        every_nth: BlockCount = BlockCount(1),
        polling_interval: timedelta = timedelta(seconds=1),
        pylon_client: PylonClient,
    ) -> None:
        """
        Args:
            _id: The node ID / name
            every_nth: Emit only every n-th block
            polling_interval: How often to poll for the latest block
            pylon_client: The Pylon client to use for polling
        """
        super().__init__(_id)
        self.source = Source(_id)
        self.every_nth = every_nth
        self.polling_interval = polling_interval
        self.pylon_client: PylonClient = pylon_client

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> BlockBeatActor:
        return BlockBeatActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)

    def sinks(self) -> NodeSinks:
        return NodeSinks({})

    def sources(self) -> NodeSources:
        return NodeSources({SourceName("block-beat"): self.source})


class BlockBeatActor(ProducerActor[BlockBeat]):
    def __init__(self, spec: BlockBeatNode, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(source=spec.source, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec
        self._stop_event = Event()

    @override
    def _stop(self) -> None:
        self._stop_event.set()

    @override
    def _produce(self) -> Generator[BlockBeat]:
        last_emitted: BlockNumber | None = None
        pylon = self.spec.pylon_client
        interval_seconds = self.spec.polling_interval.total_seconds()

        while not self._stop_event.is_set():
            poll_start = time.monotonic()
            response = pylon.open_access.get_latest_block_info()
            block_number = BlockNumber(response.number)

            if block_number != last_emitted and block_number % self.spec.every_nth == 0:
                logger.info(f"New block: {block_number}")
                last_emitted = block_number
                yield BlockBeat(
                    block_number=block_number,
                    block_timestamp=BlockTimestamp(response.timestamp),
                    block_hash=BlockHash(response.hash),
                )

            remaining = interval_seconds - (time.monotonic() - poll_start)
            if remaining > 0:
                self._stop_event.wait(timeout=remaining)
