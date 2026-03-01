# pyright: basic
from collections.abc import Callable
from threading import Thread
from typing import Any

from polyfactory.factories.pydantic_factory import ModelFactory
from pylon_client.artanis.v1 import Neuron
from tenacity import RetryError, retry, stop_after_delay, wait_fixed

from nexus.actors.chain_beat.block_beat import BlockBeat
from nexus.actors.chain_beat.epoch_beat import EpochBeat
from nexus.core.dsl.nodes import Sink
from nexus.core.runtime.actor import Actor, EventHandler
from nexus.core.runtime.context_store import Context, ContextStore, InMemoryContextStorePersistence
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent
from nexus.utils.chain import get_epoch_containing_block
from nexus.utils.types import BlockHash, BlockNumber, NetUid, Timestamp


class NeuronFactory(ModelFactory[Neuron]):
    __model__ = Neuron


def build_neuron(
    *,
    uid: int,
    hotkey: str,
    validator_permit: bool,
) -> Neuron:
    return NeuronFactory.build(
        uid=uid,
        hotkey=hotkey,
        coldkey=f"cold-{hotkey}",
        validator_permit=validator_permit,
    )


def wait_until(condition: Callable[[], bool], *, timeout=1.0, interval=0.05):
    """
    I wasn't able to find this as a library function :shrug:

    Wait until the given condition callable returns True, or raise an AssertionError if the timeout is reached.
    """  # noqa: DOC501

    @retry(stop=stop_after_delay(timeout), wait=wait_fixed(interval), reraise=True)
    def _check():
        if not condition():
            raise AssertionError("Condition not yet true")

    try:
        _check()
    except RetryError as exc:
        raise AssertionError(f"Condition not met within {timeout} seconds") from exc.last_attempt.exception()


class Jobs:
    def __init__(self, *jobs: Thread):
        self.jobs = jobs

    def join(self, timeout=1.0):
        for job in self.jobs:
            job.join(timeout)
            assert not job.is_alive()


class CollectorActor[T](Actor):
    sink: Sink[T]
    received_events: list[ReceiveEvent[T]]

    def __init__(
        self,
        *,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
        name: str = "collector",
    ) -> None:
        super().__init__(name=name, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.sink = Sink[T](f"{name}-sink")
        self.received_events = []

    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.sink: self._handle}

    def _handle(self, _: Context, event: ReceiveEvent[T]) -> MessagesToSend:
        self.received_events.append(event)
        return ()


def empty_context_store() -> ContextStore:
    persistence = InMemoryContextStorePersistence()
    return ContextStore.recover_from(persistence).context_store


def dummy_epoch_beat(block_number: BlockNumber, netuid: NetUid) -> EpochBeat:
    return EpochBeat(epoch=get_epoch_containing_block(block_number, netuid=netuid))


def dummy_block_beat(block_number: BlockNumber) -> BlockBeat:
    return BlockBeat(
        block_number=BlockNumber(block_number),
        block_timestamp=Timestamp(block_number * 1000),
        block_hash=BlockHash(f"0x{block_number:064x}"),
    )
