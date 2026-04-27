from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

import httpx
import uvicorn
from litestar import Litestar, get, post
from litestar.enums import MediaType
from litestar.response import Response
from pydantic import AnyHttpUrl, BaseModel, ValidationError
from pylon_client.artanis import Port

from nexus._internal.logging_utils import get_logger
from nexus._internal.utils.exceptions import ActorMisconfiguredException, RemoteRequestFailedException

from .async_http_protocol import (
    AsyncHttpNeuronRequestEnvelope,
    AsyncHttpNeuronResponseEnvelope,
    RequestId,
)
from .common import NormalizedHttpPath, normalize_http_path, timeout_seconds, validate_positive_timeout
from .http_server_lifecycle import start_uvicorn_server, stop_uvicorn_server

logger = get_logger(__name__)


class AsyncHttpNeuronService[InputModel: BaseModel, OutputModel: BaseModel]:
    """
    A minimal remote neuron-like service that receives communicator requests,
    executes a provided Input->Output processor, and sends callbacks back.
    """

    _STARTUP_TIMEOUT: timedelta = timedelta(seconds=1)
    _STARTUP_POLL_INTERVAL: timedelta = timedelta(milliseconds=25)
    _STARTUP_FAILURE_JOIN_TIMEOUT_SECONDS: float = 1.0
    _SHUTDOWN_JOIN_TIMEOUT_SECONDS: float = 1.0
    _KEEP_ALIVE_TIMEOUT_SECONDS: int = 5

    path: NormalizedHttpPath
    port: Port
    input_model: type[InputModel]
    output_model: type[OutputModel]
    processor: Callable[[InputModel], OutputModel]
    callback_timeout: timedelta

    _server: uvicorn.Server | None
    _server_thread: threading.Thread | None
    _bound_port: Port | None
    _worker_threads: set[threading.Thread]
    _worker_threads_lock: threading.Lock

    def __init__(
        self,
        *,
        path: str,
        port: Port,
        input_model: type[InputModel],
        output_model: type[OutputModel],
        processor: Callable[[InputModel], OutputModel],
        callback_timeout: timedelta = timedelta(seconds=5),
    ) -> None:
        if not (0 <= int(port) <= 65535):
            raise ActorMisconfiguredException("port must be in [0, 65535]")
        validate_positive_timeout(timeout=callback_timeout, parameter_name="callback_timeout")
        self.path = normalize_http_path(path)
        self.port = port
        self.input_model = input_model
        self.output_model = output_model
        self.processor = processor
        self.callback_timeout = callback_timeout
        self._server = None
        self._server_thread = None
        self._bound_port = None
        self._worker_threads = set()
        self._worker_threads_lock = threading.Lock()

    @property
    def bound_port(self) -> Port:
        if self._bound_port is None:
            raise RuntimeError("Service is not started.")
        return self._bound_port

    def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("Service is already started.")

        app = self._build_litestar_app()
        server, server_thread, bound_port = start_uvicorn_server(
            app=app,
            host="0.0.0.0",
            port=self.port,
            thread_name="AsyncHttpNeuronServiceHTTP",
            keep_alive_timeout_seconds=self._KEEP_ALIVE_TIMEOUT_SECONDS,
            startup_timeout=self._STARTUP_TIMEOUT,
            startup_poll_interval=self._STARTUP_POLL_INTERVAL,
            startup_failure_join_timeout_seconds=self._STARTUP_FAILURE_JOIN_TIMEOUT_SECONDS,
            server_name="AsyncHttpNeuronService",
        )

        self._server = server
        self._server_thread = server_thread
        self._bound_port = bound_port
        logger.info("AsyncHttpNeuronService listening on port=%s path=%r", self.bound_port, self.path)

    def stop(self) -> None:
        server = self._server
        server_thread = self._server_thread
        self._server = None
        self._server_thread = None
        self._bound_port = None

        stop_uvicorn_server(
            server=server,
            server_thread=server_thread,
            shutdown_join_timeout_seconds=self._SHUTDOWN_JOIN_TIMEOUT_SECONDS,
            timeout_warning_message="AsyncHttpNeuronService server thread did not stop within timeout.",
            logger=logger,
        )

    @contextmanager
    def running(self):
        self.start()
        try:
            yield self
        finally:
            self.stop()

    def _build_litestar_app(self) -> Any:
        @post(path=["/", "/{request_path:path}"])
        async def handle_post(request: Any, request_path: str | None = None) -> Response[str]:
            del request_path  # route parameter only, path matching remains explicit.
            if request.url.path != self.path:
                return self._text_response(status=404, body="Not Found\n")
            return await self._handle_post(request=request)

        @get(path=["/", "/{request_path:path}"])
        async def handle_get(request_path: str | None = None) -> Response[str]:
            del request_path
            return self._text_response(status=405, body="Method Not Allowed\n")

        return Litestar(route_handlers=[handle_post, handle_get])

    async def _handle_post(self, *, request: Any) -> Response[str]:
        content_length_header = request.headers.get("content-length")
        if content_length_header is None:
            return self._text_response(status=411, body="Content-Length required\n")

        try:
            content_length = int(content_length_header)
        except ValueError:
            return self._text_response(status=400, body="Invalid Content-Length\n")

        try:
            raw_body = await request.body()
        except Exception as exc:
            logger.warning("Failed reading AsyncHttpNeuronService request body.", exc_info=exc)
            return self._text_response(status=400, body="Failed reading request body\n")

        if content_length != len(raw_body):
            return self._text_response(status=400, body="Invalid Content-Length\n")

        try:
            envelope = AsyncHttpNeuronRequestEnvelope.model_validate_json(raw_body)
            input_payload = self.input_model.model_validate(envelope.input)
        except ValidationError as exc:
            return self._text_response(status=400, body=f"Invalid request body: {exc}\n")
        except Exception as exc:
            logger.warning("Failed parsing AsyncHttpNeuronService request body.", exc_info=exc)
            return self._text_response(status=400, body="Invalid request body\n")

        worker = threading.Thread(
            target=self._process_and_callback,
            kwargs={
                "request_id": envelope.request_id,
                "callback_url": envelope.callback_url,
                "input_payload": input_payload,
            },
            daemon=True,
            name=f"AsyncHttpNeuronServiceWorker-{envelope.request_id}",
        )
        with self._worker_threads_lock:
            self._worker_threads.add(worker)
        worker.start()

        return self._text_response(status=202, body="accepted\n")

    def _process_and_callback(
        self,
        *,
        request_id: RequestId,
        callback_url: AnyHttpUrl,
        input_payload: InputModel,
    ) -> None:
        try:
            callback_body = self._build_callback_body(
                request_id=request_id,
                input_payload=input_payload,
            )
            try:
                self._post_callback(callback_url=callback_url, callback_body=callback_body)
            except Exception as exc:
                logger.warning(
                    "Failed sending callback for request_id=%s to callback_url=%r",
                    request_id,
                    callback_url,
                    exc_info=exc,
                )
        finally:
            with self._worker_threads_lock:
                self._worker_threads.discard(threading.current_thread())

    def _build_callback_body(self, *, request_id: RequestId, input_payload: InputModel) -> str:
        try:
            output = self.processor(input_payload)
            validated_output = self.output_model.model_validate(output)
            callback_payload = AsyncHttpNeuronResponseEnvelope(
                request_id=request_id,
                output=validated_output.model_dump(mode="json"),
                error=None,
            )
        except Exception as exc:
            callback_payload = AsyncHttpNeuronResponseEnvelope(
                request_id=request_id,
                output=None,
                error=f"Remote processing failed: {exc!r}",
            )
        return callback_payload.model_dump_json()

    def _post_callback(self, *, callback_url: AnyHttpUrl, callback_body: str) -> None:
        callback_url_text = str(callback_url)
        parsed_callback = httpx.URL(callback_url_text)
        if parsed_callback.scheme != "http":
            raise RemoteRequestFailedException(f"Unsupported callback URL scheme: {parsed_callback.scheme!r}")
        if parsed_callback.port is None:
            raise RemoteRequestFailedException(f"Callback URL missing host/port: {callback_url_text!r}")

        payload = callback_body.encode("utf-8")
        callback_timeout = httpx.Timeout(timeout_seconds(self.callback_timeout))
        try:
            with httpx.Client(timeout=callback_timeout) as callback_client:
                response = callback_client.post(
                    callback_url_text,
                    content=payload,
                    headers={
                        "Content-Type": "application/json; charset=utf-8",
                    },
                )
                _ = response.content
        except httpx.TimeoutException as exc:
            raise RemoteRequestFailedException(
                f"Timeout while sending callback request to {callback_url_text!r}"
            ) from exc
        except httpx.RequestError as exc:
            raise RemoteRequestFailedException(
                f"Failed sending callback request to {callback_url_text!r}: {exc!r}"
            ) from exc

    @staticmethod
    def _text_response(*, status: int, body: str) -> Response[str]:
        return Response(
            content=body,
            status_code=status,
            media_type=MediaType.TEXT,
        )
