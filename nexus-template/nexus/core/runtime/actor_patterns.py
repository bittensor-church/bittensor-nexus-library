from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, cast, override

from .actor import Actor, EventHandler
from .context_store import ContextStore, Context
from .events import PipeToBus, ReceiveEvent, SendEvent, MessagesToSend
from ..dsl.nodes import Fork, Sink, Source, Transform


def _safe_invoke[ReturnType](fn: Callable[[], ReturnType]) -> tuple[ReturnType, None] | tuple[None, Exception]:
    try:
        return fn(), None
    except Exception as exception:
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
        assert left_payload is not None
        return (SendEvent(ctx_id=event.ctx_id, source=left, payload=left_payload),)
    if left_payload is None:
        return (SendEvent(ctx_id=event.ctx_id, source=right, payload=right_payload),)
    raise AssertionError(f"Unexpected fork handler output for event {event}: {(left_payload, right_payload)}")


class ConsumerActor[From](Actor, ABC):
    def __init__(self, spec: Sink[From], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {self.spec: self.handle}

    def handle(self, ctx: Context, event: ReceiveEvent[Any]) -> MessagesToSend:
        assert event.target == self.spec
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
        assert event.target == self.spec.sink
        return _fork_handler(ctx, event, self._process, self.spec.left, self.spec.right)

    @abstractmethod
    def _process(self, ctx: Context, payload: From) -> tuple[ToLeft, None] | tuple[None, ToRight]:
        pass


class TransformActor[From, To](ForkActor[From, To, Exception], ABC):
    spec: Transform[From, To]

    def __init__(self, spec: Transform[From, To], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)

    @override
    def _process(self, ctx: Context, payload: From) -> tuple[To, None] | tuple[None, Exception]:
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
                assert event.target == self.input_spec.sink
                return _fork_handler(
                    ctx,
                    cast(ReceiveEvent[InputFrom], event),
                    lambda _ctx, payload: _safe_invoke(lambda: self._transform_input(_ctx, payload)),
                    self.input_spec.ok,
                    self.input_spec.error,
                )
            case DoubleTransformActor.Output():
                assert event.target == self.output_spec.sink
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
