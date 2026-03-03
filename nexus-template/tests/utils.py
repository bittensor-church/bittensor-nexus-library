# pyright: basic
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import Thread
from typing import Any, override
from unittest.mock import MagicMock, create_autospec, seal

from polyfactory.factories.pydantic_factory import ModelFactory
from pylon_client.artanis.v1 import Neuron
from tenacity import RetryError, retry, stop_after_delay, wait_fixed

from nexus.actors import Timestamped
from nexus.actors.chain_beat.block_beat import BlockBeat
from nexus.actors.chain_beat.epoch_beat import EpochBeat
from nexus.actors.executor_communicator import ProcessedInput
from nexus.actors.neuron_router import Routed
from nexus.actors.pylon_client_provider import OpenAccessPylonApiLike, PylonClientProvider, SyncPylonClientLike
from nexus.actors.task_result_store_provider import TaskResultStoreProvider
from nexus.core.dsl.nodes import Sink
from nexus.core.runtime.actor import Actor, EventHandler
from nexus.core.runtime.context_store import Context, ContextStore, InMemoryContextStorePersistence
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent
from nexus.core.runtime.nexus_task_types import NexusTaskName, TaskResultId
from nexus.core.runtime.task_result_store import (
    InMemoryTaskResultStore,
    SingleTaskResult,
    StoredTaskExecution,
    TaskResultStore,
)
from nexus.utils.chain import get_epoch_containing_block
from nexus.utils.exceptions import NexusException
from nexus.utils.types import BlockHash, BlockNumber, NetUid, Timestamp

DEFAULT_TEST_NETUID = NetUid(1)


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


def dummy_block_beat(block_number: BlockNumber | int) -> BlockBeat:
    return BlockBeat(
        block_number=BlockNumber(block_number),
        block_timestamp=Timestamp(block_number * 1000),
        block_hash=BlockHash(f"0x{block_number:064x}"),
    )


class InMemoryTestTaskResultStoreProvider[ExecutorPayload, Output](TaskResultStoreProvider[ExecutorPayload, Output]):
    """TaskResultStoreProvider with isolated in-memory state per test setup."""

    _store: TaskResultStore[ExecutorPayload, Output]

    def __init__(self) -> None:
        self._store = InMemoryTaskResultStore[ExecutorPayload, Output]()

    @override
    def get_task_result_store(self) -> TaskResultStore[ExecutorPayload, Output]:
        return self._store


def get_stored_results_for_block[ExecutorPayload, Output](
    *,
    store: TaskResultStore[ExecutorPayload, Output],
    task_name: NexusTaskName,
    block_number: int,
    netuid: NetUid = DEFAULT_TEST_NETUID,
) -> tuple[SingleTaskResult[ExecutorPayload, Output], ...]:
    epoch = get_epoch_containing_block(BlockNumber(block_number), netuid=netuid)
    return store.get_tasks_for_epoch(task_name, epoch)


def build_nexus_task_result[ExecutorPayload, Output](
    *,
    executor_payload: ExecutorPayload,
    output: Output | NexusException,
    block_number: int,
    target_hotkey: str,
    target_uid: int = 1,
    target_validator_permit: bool = False,
    processing_started: datetime | None = None,
    processing_finished: datetime | None = None,
) -> StoredTaskExecution[ExecutorPayload, Output]:
    if processing_started is None:
        processing_started = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    if processing_finished is None:
        processing_finished = processing_started + timedelta(seconds=1)

    return Timestamped(
        executor_output=ProcessedInput(
            input=Routed(
                input=executor_payload,
                target=build_neuron(
                    uid=target_uid,
                    hotkey=target_hotkey,
                    validator_permit=target_validator_permit,
                ),
            ),
            output=output,
        ),
        processing_started=processing_started,
        processing_finished=processing_finished,
        block_at_finish=dummy_block_beat(block_number),
    )


def store_nexus_task_result[ExecutorPayload, Output](
    *,
    context_store: ContextStore,
    task_result_store: TaskResultStore[ExecutorPayload, Output],
    task_name: NexusTaskName,
    result: StoredTaskExecution[ExecutorPayload, Output],
) -> TaskResultId:
    with context_store.create_context() as ctx:
        return task_result_store.add_task_result(ctx=ctx, task_name=task_name, result=result)


class MockPylonClientProvider(PylonClientProvider):
    """Provides a mock pylon client for testing beat actors.

    Use prepare_mock_client() to create and configure the mock before the actor runs.
    """

    _client: MagicMock | None

    def __init__(self) -> None:
        self._client = None

    @contextmanager
    def prepare_mock_client(self) -> Generator[MagicMock]:
        """Create an autospec'd mock client, seal it, and yield for configuration."""
        client = create_autospec(SyncPylonClientLike, instance=True)
        # autospec doesn't recurse into Protocol property return types
        client.open_access = create_autospec(OpenAccessPylonApiLike, instance=True)
        # autospec creates dunder methods as lazy descriptors; seal() blocks lazy creation.
        # Force-realize them before sealing.
        client.__enter__.return_value = client
        client.__exit__.return_value = None
        seal(client)
        yield client
        self._client = client

    @override
    def get_client(self) -> SyncPylonClientLike:
        assert self._client is not None, "Call prepare_client() before get_client()"
        return self._client  # type: ignore[return-value]
