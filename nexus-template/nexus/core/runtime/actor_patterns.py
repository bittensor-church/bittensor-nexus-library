import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator
from threading import Thread
from typing import Any, cast, override

from nexus.logging_utils import get_logger
from nexus.utils.exceptions import InternalFrameworkException, NexusException, SafeInvokeWrappedException

from ..dsl.nodes import Fork, Producer, Sink, Source, Transform
from .actor import Actor, EventHandler
from .context_store import Context, ContextStore
from .events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent

logger: logging.Logger = get_logger(__name__)


def _safe_invoke[ReturnType](fn: Callable[[], ReturnType]) -> tuple[ReturnType, None] | tuple[None, NexusException]:
    try:
        try:
            return fn(), None
        except NexusException:
            raise
        except Exception as exception:
            raise SafeInvokeWrappedException() from exception
    except NexusException as exception:
        return None, exception


def _fork_handler[From, ToLeft, ToRight](
    ctx: Context,
    event: ReceiveEvent[From],
    process: Callable[[Context, From], tuple[ToLeft, None] | tuple[None, ToRight]],
    left: Source[ToLeft],
    right: Source[ToRight],
) -> tuple[SendEvent[ToLeft] | SendEvent[ToRight]]:
    left_payload, right_payload = process(ctx, event.payload)
    if right_payload is None:
        if left_payload is None:
            raise InternalFrameworkException("no payload to process in fork handler")
        return (SendEvent(ctx_id=event.ctx_id, source=left, payload=left_payload),)
    if left_payload is None:
        return (SendEvent(ctx_id=event.ctx_id, source=right, payload=right_payload),)
    raise InternalFrameworkException(
        f"Unexpected fork handler output for event {event}: {(left_payload, right_payload)}"
    )


class ProducerActor[Product](Actor, ABC):
    """
    Source-only actor that originates events on its own and emits them from a single source.

    Runs _produce() in a background daemon thread while the main actor thread watches for the framework stop signal.

    It is expected that it will loop and sleep as necessary while respecting some form of a stop signal:
     - For a blocking resource-backed producer like a WS listener, on_stop() may close the underlying resource.
     - For sleep-based polling producers, the loop may sleep on a threading.Event while on_stop() should set the event.

    The producer thread is a daemon and will be killed on process exit if on_stop() fails to unblock it.
    """

    spec: Producer[Product]
    _pipe_to_bus: PipeToBus
    producer_thread: Thread | None

    def __init__(self, spec: Producer[Product], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec
        self._pipe_to_bus = pipe_to_bus
        self.producer_thread = None

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        # Control sink makes the actor visible to the event bus for lifecycle signals (e.g. StopActorEvent)
        # but isn't used for anything else
        return {self.spec.sink: lambda _ctx, _event: ()}

    @override
    def on_start(self) -> None:
        if self.thread is None:
            raise InternalFrameworkException(f"{self.actor_id} on_start called before thread was assigned")

        self.producer_thread = Thread(
            target=self._producer_loop,
            daemon=True,
            name=f"{self.thread.name}-producer",  # Inherit main actor thread name as a prefix
        )
        self.producer_thread.start()

    def _producer_loop(self) -> None:
        try:
            for product in self._produce():
                with self.context_store.create_context() as ctx:
                    ctx_id = ctx.id
                self._pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=self.spec.source, payload=product))

        except Exception as exc:
            # As this is a side thread, let's always leave a mark when it exits unexpectedly as we don't know whether
            # the parent will be listening for failures.
            logger.error(f"{self.actor_id} producer thread failed", exc_info=exc)
            raise

    @abstractmethod
    def _produce(self) -> Generator[Product]:
        pass


class ConsumerActor[From](Actor, ABC):
    def __init__(self, spec: Sink[From], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.spec: self.handle}

    def handle(self, ctx: Context, event: ReceiveEvent[Any]) -> MessagesToSend:
        if event.target != self.spec:
            raise InternalFrameworkException("event target does not match consumer actor's sink")
        self._consume(ctx, event.payload)
        return ()

    @abstractmethod
    def _consume(self, ctx: Context, payload: From) -> None:
        pass


class ForkActor[From, ToLeft, ToRight](Actor, ABC):
    def __init__(self, spec: Fork[From, ToLeft, ToRight], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec

    @override
    def handlers(self) -> dict[Sink[From], EventHandler]:
        return {self.spec.sink: self.handle}

    def handle(self, ctx: Context, event: ReceiveEvent[From]) -> MessagesToSend:
        if event.target != self.spec.sink:
            raise InternalFrameworkException("event target does not mach fork actor sink")
        return _fork_handler(ctx, event, self._process, self.spec.left, self.spec.right)

    @abstractmethod
    def _process(self, ctx: Context, payload: From) -> tuple[ToLeft, None] | tuple[None, ToRight]:
        pass


class TransformActor[From, To](ForkActor[From, To, NexusException], ABC):
    spec: Transform[From, To]

    def __init__(self, spec: Transform[From, To], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)

    @override
    def _process(self, ctx: Context, payload: From) -> tuple[To, None] | tuple[None, NexusException]:
        return _safe_invoke(lambda: self._transform(ctx, payload))

    @abstractmethod
    def _transform(self, ctx: Context, payload: From) -> To:
        pass


class DoubleTransformActor[InputFrom, InputTo, OutputFrom, OutputTo](Actor, ABC):
    class Input:
        pass

    class Output:
        pass

    input_spec: Transform[InputFrom, InputTo]
    output_spec: Transform[OutputFrom, OutputTo]

    def __init__(
        self,
        name: str,
        input_spec: Transform[InputFrom, InputTo],
        output_spec: Transform[OutputFrom, OutputTo],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(name=name, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.input_spec = input_spec
        self.output_spec = output_spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.input_spec.sink: lambda ctx, event: self.handle(DoubleTransformActor.Input(), ctx, event),
            self.output_spec.sink: lambda ctx, event: self.handle(DoubleTransformActor.Output(), ctx, event),
        }

    def handle(self, pipe: Input | Output, ctx: Context, event: ReceiveEvent[Any]) -> MessagesToSend:
        match pipe:
            case DoubleTransformActor.Input():
                if event.target != self.input_spec.sink:
                    raise InternalFrameworkException("event target does not match double transform actor input sink")
                return _fork_handler(
                    ctx,
                    cast(ReceiveEvent[InputFrom], event),
                    lambda _ctx, payload: _safe_invoke(lambda: self._transform_input(_ctx, payload)),
                    self.input_spec.ok,
                    self.input_spec.error,
                )
            case DoubleTransformActor.Output():
                if event.target != self.output_spec.sink:
                    raise InternalFrameworkException("event target does not match double transform actor output sink")
                return _fork_handler(
                    ctx,
                    cast(ReceiveEvent[OutputFrom], event),
                    lambda _ctx, payload: _safe_invoke(lambda: self._transform_output(_ctx, payload)),
                    self.output_spec.ok,
                    self.output_spec.error,
                )

    @abstractmethod
    def _transform_input(self, ctx: Context, payload: InputFrom) -> InputTo:
        pass

    @abstractmethod
    def _transform_output(self, ctx: Context, payload: OutputFrom) -> OutputTo:
        pass
