from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, cast, override

from nexus.context_store import ContextId

from ..dsl.nodes import Fork, Sink, Source, Transform
from .actor import Actor, EventHandler
from .events import PipeToBus, ReceiveEvent, SendEvent


def _safe_invoke[ReturnType](fn: Callable[[], ReturnType]) -> tuple[ReturnType, None] | tuple[None, Exception]:
    try:
        return fn(), None
    except Exception as exception:
        return None, exception


def _fork_handler[From, ToLeft, ToRight](
        event: ReceiveEvent[From],
        process: Callable[[ContextId, From], tuple[ToLeft, None] | tuple[None, ToRight]],
        left: Source[ToLeft],
        right: Source[ToRight],
        pipe_to_bus: PipeToBus) -> None:
    result = process(event.ctx, event.payload)
    match result:
        case (left_payload, None):
            pipe_to_bus.put(
                SendEvent(ctx=event.ctx, source=left, payload=left_payload))
        case (None, right_payload):
            pipe_to_bus.put(
                SendEvent(ctx=event.ctx, source=right, payload=right_payload))


class ConsumerActor[From](Actor, ABC):
    def __init__(self,
                 spec: Sink[From],
                 pipe_to_bus: PipeToBus) -> None:
        super().__init__(name=spec.gid, pipe_to_bus=pipe_to_bus)
        self.spec = spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec: self.handle
        }

    def handle(self, event: ReceiveEvent[Any]) -> None:
        assert event.target == self.spec
        return self._consume(event.ctx, event.payload)

    @abstractmethod
    def _consume(self, ctx: ContextId, payload: From) -> None:
        pass


class ForkActor[From, ToLeft, ToRight](Actor, ABC):
    def __init__(self,
                 spec: Fork[From, ToLeft, ToRight],
                 pipe_to_bus: PipeToBus) -> None:
        super().__init__(name=spec.gid, pipe_to_bus=pipe_to_bus)
        self.spec = spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec.sink: self.handle
        }

    def handle(self, event: ReceiveEvent[Any]) -> None:
        assert event.target == self.spec.sink
        return _fork_handler(cast(ReceiveEvent[From], event),
                             self._process,
                             self.spec.left,
                             self.spec.right,
                             self.pipe_to_bus)

    @abstractmethod
    def _process(self, ctx: ContextId, payload: From) -> tuple[ToLeft, None] | tuple[None, ToRight]:
        pass


class TransformActor[From, To](ForkActor[From, To, Exception], ABC):
    spec: Transform[From, To]

    def __init__(self, spec: Transform[From, To], pipe_to_bus: PipeToBus) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus)

    @override
    def _process(self, ctx: ContextId, payload: From) -> tuple[To, None] | tuple[None, Exception]:
        return _safe_invoke(lambda: self._transform(ctx, payload))

    @abstractmethod
    def _transform(self, ctx: ContextId, payload: From) -> To:
        pass


class DoubleTransformActor[InputFrom, InputTo, OutputFrom, OutputTo](Actor, ABC):
    class Input:
        pass

    class Output:
        pass

    input_spec: Transform[InputFrom, InputTo]
    output_spec: Transform[OutputFrom, OutputTo]

    def __init__(self,
                 name: str,
                 input_spec: Transform[InputFrom, InputTo],
                 output_spec: Transform[OutputFrom, OutputTo],
                 pipe_to_bus: PipeToBus) -> None:
        super().__init__(name=name, pipe_to_bus=pipe_to_bus)
        self.input_spec = input_spec
        self.output_spec = output_spec

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.input_spec.sink: lambda event: self.handle(DoubleTransformActor.Input(), event),
            self.output_spec.sink: lambda event: self.handle(DoubleTransformActor.Output(), event),
        }

    def handle(self, pipe: Input | Output, event: ReceiveEvent[Any]) -> None:
        match pipe:
            case DoubleTransformActor.Input():
                assert event.target == self.input_spec.sink
                return _fork_handler(cast(ReceiveEvent[InputFrom], event),
                                     lambda ctx, payload: _safe_invoke(lambda: self._transform_input(ctx, payload)),
                                     self.input_spec.ok,
                                     self.input_spec.error,
                                     self.pipe_to_bus)
            case DoubleTransformActor.Output():
                assert event.target == self.output_spec.sink
                return _fork_handler(cast(ReceiveEvent[OutputFrom], event),
                                     lambda ctx, payload: _safe_invoke(lambda: self._transform_output(ctx, payload)),
                                     self.output_spec.ok,
                                     self.output_spec.error,
                                     self.pipe_to_bus)

    @abstractmethod
    def _transform_input(self, ctx: ContextId, payload: InputFrom) -> InputTo:
        pass

    @abstractmethod
    def _transform_output(self, ctx: ContextId, payload: OutputFrom) -> OutputTo:
        pass
