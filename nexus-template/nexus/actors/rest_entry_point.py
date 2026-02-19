from __future__ import annotations

import logging
import queue
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, override
from urllib.parse import urlparse

from pydantic import BaseModel, ValidationError

from nexus.core.dsl.nodes import Node, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus.core.runtime.actor import Actor, ActorBuilder, EventHandler
from nexus.core.runtime.context_store import Context, ContextId, ContextStore
from nexus.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent, SendEvent
from nexus.logging_utils import get_logger

logger: logging.Logger = get_logger(__name__)


class RestEntryPoint[Model: BaseModel](Node, ActorBuilder):
    source: Source[Model]
    sink: Sink[str]
    path: str
    port: int
    user_data_model: type[Model]

    def __init__(self, *, _id: str, path: str, port: int, user_data_model: type[Model]) -> None:
        super().__init__(_id = _id)
        self.path = path if path.startswith("/") else f"/{path}"
        self.port = port
        self.user_data_model = user_data_model
        self.source = Source(f"{self.id}-source")
        self.sink = Sink(f"{self.id}-sink")

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

        self._pending_by_ctx_id: dict[ContextId, queue.Queue[str]] = {}
        self._pending_lock = threading.Lock()

        self._server: ThreadingHTTPServer | None = None
        self._server_thread: threading.Thread | None = None

    @override
    def handlers(self) -> dict[Sink[Any], EventHandler]:
        return {
            self.spec.sink: self._handle_response,
        }

    @override
    def run_loop(self) -> threading.Thread:
        self._ensure_server_started()
        return super().run_loop()

    @override
    def _loop(self) -> None:
        self._ensure_server_started()
        try:
            super()._loop()
        finally:
            self._stop_server()
            
    def _ensure_server_started(self) -> None:
        if self._server_thread is not None:
            return

        handler_cls = self._make_http_handler()
        server = ThreadingHTTPServer(("", self.spec.port), handler_cls)
        server.daemon_threads = True
        self._server = server

        t = threading.Thread(
            target=server.serve_forever,
            daemon=True,
            name=f"RestEntryPointHTTP-{self.spec.id}",
        )
        t.start()
        self._server_thread = t
        bound_port = server.server_address[1]
        logger.info(f"RestEntryPoint listening on port={bound_port} path={self.spec.path!r}")

    def _stop_server(self) -> None:
        server = self._server
        if server is None:
            return
        try:
            server.shutdown()
        except Exception as exc:
            logger.warning("Failed to shutdown RestEntryPoint HTTP server cleanly", exc_info=exc)
        try:
            server.server_close()
        except Exception as exc:
            logger.warning("Failed to close RestEntryPoint HTTP server cleanly", exc_info=exc)
        self._server = None
        self._server_thread = None

    def _handle_response(self, context: Context, event: ReceiveEvent[Any]) -> MessagesToSend:
        ctx_id = context.id
        if not isinstance(event.payload, str):
            logger.error(f"RestEntryPoint expected str response, got {type(event.payload)!r} for ctx={ctx_id}")
            return ()

        with self._pending_lock:
            response_queue = self._pending_by_ctx_id.get(ctx_id)

        if response_queue is None:
            logger.warning(f"No pending HTTP request found for ctx={ctx_id}; dropping response.")
            return ()

        try:
            response_queue.put_nowait(event.payload)
        except queue.Full:
            logger.warning(f"Multiple responses received for ctx={ctx_id}; dropping subsequent response.")
        return ()

    def _make_http_handler(self) -> type[BaseHTTPRequestHandler]:
        actor = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self) -> None:  # noqa: N802 (http.server convention)
                actor._handle_post(self)

            def do_GET(self) -> None:  # noqa: N802 (http.server convention)
                actor._send_text(self, status=405, body="Method Not Allowed\n")

            def log_message(self, format: str, *args: Any) -> None:
                try:
                    message = format % args
                except Exception:
                    message = format
                logger.info("%s - %s", self.client_address[0], message)

        return Handler

    def _handle_post(self, request: BaseHTTPRequestHandler) -> None:
        request_path = urlparse(request.path).path
        if request_path != self.spec.path:
            self._send_text(request, status=404, body="Not Found\n")
            return

        content_length_raw = request.headers.get("Content-Length")
        if content_length_raw is None:
            self._send_text(request, status=411, body="Content-Length required\n")
            return

        try:
            content_length = int(content_length_raw)
        except ValueError:
            self._send_text(request, status=400, body="Invalid Content-Length\n")
            return

        try:
            body = request.rfile.read(content_length) if content_length > 0 else b""
        except Exception as exc:
            logger.warning("Failed reading HTTP request body", exc_info=exc)
            self._send_text(request, status=400, body="Failed to read request body\n")
            return

        try:
            model = self.spec.user_data_model.model_validate_json(body)
        except ValidationError as exc:
            self._send_text(request, status=400, body=f"Invalid request body: {exc}\n")
            return
        except Exception as exc:
            logger.warning("Failed parsing HTTP request body", exc_info=exc)
            self._send_text(request, status=400, body="Invalid request body\n")
            return

        with self.context_store.create_context() as context:
            ctx_id = context.id
        response_queue: queue.Queue[str] = queue.Queue(maxsize=1)

        with self._pending_lock:
            self._pending_by_ctx_id[ctx_id] = response_queue

        try:
            self._pipe_to_bus.put(SendEvent(ctx_id=ctx_id, source=self.spec.source, payload=model))
            try:
                response = response_queue.get(timeout=self._RESPONSE_TIMEOUT_S)
            except queue.Empty:
                self._send_text(request, status=504, body="Gateway Timeout\n")
                return

            self._send_text(request, status=200, body=response)
        finally:
            with self._pending_lock:
                self._pending_by_ctx_id.pop(ctx_id, None)

    @staticmethod
    def _send_text(request: BaseHTTPRequestHandler, *, status: int, body: str) -> None:
        body_bytes = body.encode("utf-8")
        request.send_response(status)
        request.send_header("Content-Type", "text/plain; charset=utf-8")
        request.send_header("Content-Length", str(len(body_bytes)))
        request.send_header("Connection", "close")
        request.end_headers()
        request.wfile.write(body_bytes)
        request.close_connection = True
