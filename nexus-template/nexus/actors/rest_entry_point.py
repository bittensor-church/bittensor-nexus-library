from __future__ import annotations

import datetime
import logging
from typing import Any, cast, override

from pydantic import BaseModel
from pydantic_core import to_jsonable_python
from pylon_client.artanis import Port

from nexus.core.dsl.nodes import Node, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent
from nexus.logging_utils import get_logger

from .executor_communicator.common import NormalizedHttpPath, normalize_http_path
from .rest_entry_point_runtime import (
    MAX_EXCEPTION_DEPTH,
    InMemoryPendingHttpResponseStore,
    JsonValue,
    PendingHttpResponse,
    PendingHttpResponseStore,
    RestEntryPointRuntime,
    RestEntryPointRuntimeConfig,
    RestEntryPointRuntimeDependencies,
    RestEntryPointRuntimeStartup,
    build_error_body,
    exception_to_error_body,
)

logger: logging.Logger = get_logger(__name__)

DEFAULT_BIND_IP = "0.0.0.0"


class RestEntryPoint[Model: BaseModel](Node, ActorBuilder):
    """Exposes an HTTP endpoint that accepts incoming requests and returns pipeline results as HTTP responses.
    Incoming requests are emitted into the pipeline via `source`. Connect the pipeline's final output
    back to `sink` to complete the request-response cycle.

    sink sink: pipeline result to return as the HTTP response
    source source: parsed request body emitted into the pipeline
    """

    source: Source[Model]
    sink: Sink[Any]
    path: NormalizedHttpPath
    port: Port
    bind_ip: str
    user_data_model: type[Model]

    def __init__(
        self,
        *,
        _id: str,
        bind_ip: str = DEFAULT_BIND_IP,
        path: str,
        port: Port | int,
        user_data_model: type[Model],
    ) -> None:
        super().__init__(_id=_id)
        self.path = normalize_http_path(path)
        self.bind_ip = bind_ip
        self.port = Port(int(port))
        self.user_data_model = user_data_model
        self.source = Source(f"{self.id}-source", owner_node=self)
        self.sink = Sink(f"{self.id}-sink", owner_node=self)

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return RestEntryPointActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks({SinkName("miner-responses"): self.sink})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(sources={SourceName("user-requests"): self.source})


class RestEntryPointActor[Model: BaseModel](Actor):
    _RESPONSE_TIMEOUT_S: float = 30.0

    def __init__(self, *, spec: RestEntryPoint[Model], pipe_to_bus: PipeToBus, context_store: ContextStore) -> None:
        super().__init__(name=spec.id, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec
        self._pending_response_store: PendingHttpResponseStore = InMemoryPendingHttpResponseStore()
        self._runtime: RestEntryPointRuntime[Model] | None = None

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec.sink: self._handle_response,
        }

    @override
    def on_start(self) -> None:
        super().on_start()
        if self._runtime is not None:
            raise RuntimeError(f"RestEntryPoint runtime already started for actor={self.actor_id}.")

        self._runtime = RestEntryPointRuntime.start(
            config=RestEntryPointRuntimeConfig(
                entry_point_id=self.spec.id,
                bind_host=self.spec.bind_ip,
                bind_port=self.spec.port,
                path=self.spec.path,
                user_data_model=self.spec.user_data_model,
                response_timeout=datetime.timedelta(seconds=self._RESPONSE_TIMEOUT_S),
            ),
            dependencies=RestEntryPointRuntimeDependencies(
                context_store=self.context_store,
                pipe_to_bus=self._pipe_to_bus,
                source=self.spec.source,
                pending_response_store=self._pending_response_store,
            ),
            startup=RestEntryPointRuntimeStartup(
                thread_name=f"RestEntryPointHTTP-{self.spec.id}",
            ),
        )

    @override
    def on_stop(self) -> None:
        super().on_stop()
        self._pending_response_store.cancel_all()
        runtime = self._runtime
        self._runtime = None
        if runtime is None:
            return
        try:
            runtime.stop()
        except Exception as exc:
            logger.warning("Failed to stop RestEntryPoint HTTP server cleanly", exc_info=exc)

    def _handle_response(self, context: Context, event: ReceiveEvent[Any]) -> MessagesToSend:
        response = self._build_http_response(event.payload)
        resolved = self._pending_response_store.resolve(context.id, response)
        if not resolved:
            logger.warning(
                "No pending HTTP request found for ctx=%s; dropping late or duplicate response payload=%r.",
                context.id,
                event.payload,
            )
        return ()

    def _build_http_response(self, payload: Any) -> PendingHttpResponse:
        if isinstance(payload, BaseException):
            return PendingHttpResponse(
                status_code=500,
                body=exception_to_error_body(payload, max_depth=MAX_EXCEPTION_DEPTH),
            )
        try:
            json_payload = cast(JsonValue, to_jsonable_python(payload))
            return PendingHttpResponse(status_code=200, body=json_payload)
        except Exception as exc:
            return PendingHttpResponse(
                status_code=500,
                body=build_error_body(
                    error_type="ResponseSerializationError",
                    message="Failed to serialize response payload to JSON.",
                    causes=((type(exc).__name__, str(exc).strip() or type(exc).__name__),),
                ),
            )
