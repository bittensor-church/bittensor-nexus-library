import logging
from threading import Thread
from typing import Any

from nexus._internal.logging_utils import get_logger

from ..dsl.nodes import Pipes, Sink
from .actor import Actor
from .context_store import ContextStore
from .context_store_types import ContextId, InvalidContextIdException
from .events import PipeToBus, ReceiveEvent, ReleaseContextEvent, SendEvent, StopActorEvent, StopBusEvent

logger: logging.Logger = get_logger(__name__)

MAX_EXCEPTION_DEPTH = 8
MAX_PAYLOAD_LOG_CHARS = 80


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
    connections: Pipes
    input_pipe: PipeToBus
    unconsumed_events_sink: Actor
    sinks: dict[Sink[Any], Actor]
    context_store: ContextStore

    def __init__(
        self, connections: Pipes, input_pipe: PipeToBus, actors: list[Actor], context_store: ContextStore
    ) -> None:
        self.connections = connections
        self.sinks = {sink: actor for actor in actors for sink in actor.handlers().keys()}
        self.input_pipe = input_pipe
        self.context_store = context_store

    def request_stop(self) -> None:
        self.input_pipe.put(StopBusEvent())

    def run_loop(self) -> Thread:
        t: Thread = Thread(target=self._loop, daemon=True, name="EventBusLoop")
        t.start()
        return t

    def _loop(self) -> None:
        while True:
            event: SendEvent[Any] = self.input_pipe.get()
            match event:
                case StopBusEvent():
                    logger.info("Stop event received in EventBus; stopping loop.")
                    for sink in self.sinks.values():
                        sink.pipe_from_bus.put(StopActorEvent())
                    self.input_pipe.task_done()
                    break
                case ReleaseContextEvent():
                    self._release(event.ctx_id)
                case _:
                    self._dispatch(event)
            self.input_pipe.task_done()

    def _dispatch(self, event: SendEvent[Any]) -> None:
        try:
            with self.context_store.get_context(event.ctx_id) as context:
                context.append_message(event.source, event.payload)
        except InvalidContextIdException as exc:
            logger.error(
                "Dropping message from %s: %s Payload: %s", event.source.id, exc, _payload_for_log(event.payload)
            )
            return
        self.pass_message_downstream(event)
        # a context that nothing references (e.g. a new one emitted to taps only) is done after its dispatch
        self.context_store._discard_if_unreferenced(event.ctx_id)  # pyright: ignore[reportPrivateUsage]

    def _release(self, ctx_id: ContextId) -> None:
        try:
            self.context_store._release(ctx_id)  # pyright: ignore[reportPrivateUsage]
        except Exception as exc:
            # an unbalanced release is a bug in the releasing actor; it must not stop the bus for everyone else
            logger.error("Failed to release context %s", ctx_id, exc_info=exc)

    def pass_message_downstream(self, event: SendEvent[Any]) -> None:
        """
        Actual message distribution logic. Recovery from the context store
        means we rebuild the contexts and then replay the messages using this function.

        Every delivery takes a reference to its context, which the receiving actor releases after handling it.
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
        pipe_from_bus = self.sinks[sink].pipe_from_bus
        self.context_store.retain(ctx_id)
        pipe_from_bus.put(ReceiveEvent(ctx_id=ctx_id, target=sink, payload=event.payload))
