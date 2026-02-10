from abc import ABC, abstractmethod
from collections.abc import Callable
from threading import Thread
from typing import Any, cast, override

from nexus.utils.exceptions import InternalFrameworkException, NexusException, SafeInvokeWrappedException

from ..dsl.nodes import Fork, Sink, Source, Transform
from .actor import Actor, EventHandler
from .context_store import Context, ContextId, ContextStore
from .events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent, StopActorEvent


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
        f"Unexpected fork handler output for event {event}: "
        f"{(left_payload, right_payload)}"
    )


class ProducerActor[To](Actor, ABC):
    """
    Source-only actor that originates events. Runs _produce() in a background daemon thread;
    the main thread waits for the framework stop signal, then calls _on_stop().

    _on_stop() is called from the main thread after the stop signal is received. Use it to
    unblock _produce() — either by closing a blocking resource (socket, subscription) or by
    setting a threading.Event that _produce() checks in its loop.

    The produce thread is a daemon and will be killed on process exit if _on_stop() fails
    to unblock it.
    """

    def __init__(self, spec: Source[To], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec
        self._pipe_to_bus = pipe_to_bus

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {}

    @override
    def _loop(self) -> None:
        produce_thread = Thread(target=self._produce, daemon=True, name=f"{type(self).__name__}-{self.actor_id}")
        produce_thread.start()
        while True:
            event = self.pipe_from_bus.get()
            self.pipe_from_bus.task_done()
            if isinstance(event, StopActorEvent):
                break
        self._on_stop()

    def _emit(self, payload: To) -> ContextId:
        with self.context_store.create_context() as ctx:
            ctx_id = ctx.id
        self._pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=self.spec, payload=payload))
        return ctx_id

    @abstractmethod
    def _on_stop(self) -> None:
        pass

    @abstractmethod
    def _produce(self) -> None:
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
