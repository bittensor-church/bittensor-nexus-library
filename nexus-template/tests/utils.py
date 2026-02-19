# pyright: basic
from threading import Thread
from typing import Any

from tenacity import RetryError, retry, stop_after_delay, wait_fixed

from nexus.core.dsl.nodes import Sink
from nexus.core.runtime.actor import Actor, EventHandler
from nexus.core.runtime.context_store import Context, ContextStore, InMemoryContextStorePersistence
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent


def wait_until(condition, *, timeout=1.0, interval=0.05):
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
    def __init__(
        self,
        *,
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
        name: str = "collector",
    ) -> None:
        super().__init__(name=name, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.sink = Sink[T](f"{name}-sink")
        self.received_events: list[ReceiveEvent[T]] = []

    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.sink: self._handle}

    def _handle(self, _: Context, event: ReceiveEvent[T]) -> MessagesToSend:
        self.received_events.append(event)
        return ()


def empty_context_store() -> ContextStore:
    persistence = InMemoryContextStorePersistence()
    return ContextStore.recover_from(persistence).context_store
