import datetime
import logging
import queue
from threading import Thread
from typing import Any

from nexus._internal.logging_utils import get_logger

from ..dsl.nodes import Pipes, Sink
from .actor import Actor
from .context_store import ContextCompletedException, ContextStore, monotonic_now
from .context_store_types import ContextId, InvalidContextIdException
from .events import PipeToBus, ReceiveEvent, SendEvent, StopActorEvent, StopBusEvent

logger: logging.Logger = get_logger(__name__)

MAX_EXCEPTION_DEPTH = 8
MAX_PAYLOAD_LOG_CHARS = 80
MAX_EXPIRY_SWEEP_INTERVAL = datetime.timedelta(minutes=1)


def _truncate(text: str, max_chars: int = MAX_PAYLOAD_LOG_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}... <truncated {len(text) - max_chars} chars>"


def _format_exception_payload(exception: BaseException) -> str:
    parts: list[str] = []
    current: BaseException | None = exception
    depth = 0
    while current is not None and depth < MAX_EXCEPTION_DEPTH:
        message = str(current).strip()
        parts.append(f"{type(current).__name__}: {message}" if message else type(current).__name__)
        current = current.__cause__ or current.__context__
        depth += 1
    if current is not None:
        parts.append("... (cause chain truncated)")
    return " <- ".join(parts)


def _payload_for_log(payload: Any) -> str:
    if isinstance(payload, BaseException):
        return _format_exception_payload(payload)
    try:
        return _truncate(repr(payload))
    except Exception as exc:
        return f"<unrepresentable payload type={type(payload).__name__}: {exc!r}>"


class EventBus:
    """
    Routes messages emitted by actors to their target sinks, recording each message in its context's log.

    With a `context_ttl`, the bus thread also expires contexts idle for longer than the TTL (see
    ContextStore.expire_idle), sweeping every TTL/10, at most every minute, also while no messages arrive. A message
    for a context that no longer exists is dropped with an error log.
    """

    connections: Pipes
    input_pipe: PipeToBus
    unconsumed_events_sink: Actor
    sinks: dict[Sink[Any], Actor]
    context_store: ContextStore
    context_ttl: datetime.timedelta | None
    _next_sweep: datetime.timedelta

    def __init__(
        self,
        connections: Pipes,
        input_pipe: PipeToBus,
        actors: list[Actor],
        context_store: ContextStore,
        context_ttl: datetime.timedelta | None = None,
    ) -> None:
        self.connections = connections
        self.sinks = {sink: actor for actor in actors for sink in actor.handlers().keys()}
        self.input_pipe = input_pipe
        self.context_store = context_store
        self.context_ttl = context_ttl
        self._next_sweep = monotonic_now()

    def request_stop(self) -> None:
        self.input_pipe.put(StopBusEvent())

    def run_loop(self) -> Thread:
        t: Thread = Thread(target=self._loop, daemon=True, name="EventBusLoop")
        t.start()
        return t

    def _loop(self) -> None:
        while True:
            event = self._next_event()
            if event is None:
                continue
            if isinstance(event, StopBusEvent):
                logger.info("Stop event received in EventBus; stopping loop.")
                for sink in self.sinks.values():
                    sink.pipe_from_bus.put(StopActorEvent())
                self.input_pipe.task_done()
                break
            self._dispatch(event)
            self.input_pipe.task_done()

    def _next_event(self) -> SendEvent[Any] | None:
        """Waits for the next event, sweeping idle contexts when due; returns None when a sweep is due."""
        if self.context_ttl is None:
            return self.input_pipe.get()
        now = monotonic_now()
        if now >= self._next_sweep:
            expired = self.context_store.expire_idle(self.context_ttl)
            logger.debug("Expired %d contexts idle for longer than %s.", expired, self.context_ttl)
            self._next_sweep = now + min(self.context_ttl / 10, MAX_EXPIRY_SWEEP_INTERVAL)
        try:
            return self.input_pipe.get(timeout=(self._next_sweep - now).total_seconds())
        except queue.Empty:
            return None

    def _dispatch(self, event: SendEvent[Any]) -> None:
        try:
            with self.context_store.get_context(event.ctx_id) as context:
                context.append_message(event.source, event.payload)
        # a context whose release failed stays registered but completed until a later sweep removes it
        except InvalidContextIdException, ContextCompletedException:
            logger.error(
                "Dropping message from source %s: context %s does not exist or is completed; it may have expired "
                "(context_ttl=%s). Payload: %s",
                event.source.id,
                event.ctx_id,
                self.context_ttl,
                _payload_for_log(event.payload),
            )
            return
        self.pass_message_downstream(event)

    def pass_message_downstream(self, event: SendEvent[Any]) -> None:
        """
        Actual message distribution logic. Recovery from the context store
        means we rebuild the contexts and then replay the messages using this function.

        Called on the bus thread right after the event touched its context; expiry runs on the same thread, so the
        context cannot expire before its tap children are created.
        """
        targets = self.connections[event.source]
        if targets.primary is None and not targets.taps:
            logger.warning(
                "No connections found for source: %s.",
                event.source.id,
            )
            return

        tap_events: list[tuple[Sink[Any], ContextId]] = []
        for tap in targets.taps:
            with self.context_store.create_context(parents=(event.ctx_id,)) as child_context:
                tap_events.append((tap, child_context.id))

        if targets.primary is not None:
            self._pass_message_to_sink(event, targets.primary, event.ctx_id)
        for tap, child_context_id in tap_events:
            self._pass_message_to_sink(event, tap, child_context_id)

    def _pass_message_to_sink[T](self, event: SendEvent[T], sink: Sink[T], ctx_id: ContextId) -> None:
        logger.debug(
            "Sending event from %s to %s with payload: %s",
            event.source.id,
            sink.id,
            _payload_for_log(event.payload),
        )
        self.sinks[sink].pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_id, target=sink, payload=event.payload))
