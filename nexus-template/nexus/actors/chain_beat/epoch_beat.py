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
from nexus.utils.chain import get_epoch_containing_block
from nexus.utils.types import BlockNumber, BlockCount, Epoch, SubnetId

logger: logging.Logger = get_logger(__name__)


@dataclass(frozen=True)
class EpochBeat:
    epoch: Epoch


class EpochBeatNode(Node, ActorBuilder):
    """
    Uses pylon, polling it in a loop to determine the current epoch for a given subnet.
    Emits a beat whenever the epoch changes.
    Keep in mind:
     - Epoch boundaries are derived from block numbers, so the same gaps and jitter
       from BlockBeatNode apply here.
     - The optional delay parameter shifts the effective block number back, useful for
       waiting until an epoch is safely finalized.
    """

    def __init__(
        self,
        _id: str,
        *,
        netuid: SubnetId,
        delay: BlockCount = BlockCount(0),
        polling_interval: timedelta = timedelta(seconds=1),
        pylon_client: PylonClient,
    ) -> None:
        """
        Args:
            _id: The node ID / name
            netuid: The subnet number for which to monitor the epoch
            delay: How many blocks after the start of an epoch to emit the beat
            polling_interval: How often to poll for the latest block number
            pylon_client: The Pylon client to use for polling
        """
        super().__init__(_id)
        self.netuid = netuid
        self.delay_blocks = delay
        self.pylon_client: PylonClient = pylon_client
        self.polling_interval = polling_interval
        self.source = Source[EpochBeat](_id)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> EpochBeatActor:
        return EpochBeatActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)

    def sinks(self) -> NodeSinks:
        return NodeSinks({})

    def sources(self) -> NodeSources:
        return NodeSources({SourceName("epoch-beat"): self.source})


class EpochBeatActor(ProducerActor[EpochBeat]):
    def __init__(self, spec: EpochBeatNode, pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(source=spec.source, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec
        self._stop_event = Event()

    @override
    def _stop(self) -> None:
        self._stop_event.set()

    @override
    def _produce(self) -> Generator[EpochBeat]:
        last_emitted: Epoch | None = None
        while not self._stop_event.is_set():
            poll_start = time.monotonic()

            response = self.spec.pylon_client.open_access.get_latest_block_info()
            current_block_number = BlockNumber(response.number)
            epoch = get_epoch_containing_block(
                block=BlockNumber(current_block_number - self.spec.delay_blocks),
                netuid=self.spec.netuid,
            )

            if epoch != last_emitted:
                logger.info(f"New epoch: {epoch}")
                last_emitted = epoch
                yield EpochBeat(epoch=epoch)

            remaining = self.spec.polling_interval.total_seconds() - (time.monotonic() - poll_start)
            if remaining > 0:
                self._stop_event.wait(timeout=remaining)
