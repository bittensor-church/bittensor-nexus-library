from __future__ import annotations

import asyncio
import datetime
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

import uvicorn
from litestar import Litestar, route
from litestar.enums import MediaType
from litestar.response import Response
from pydantic import BaseModel, ValidationError
from pylon_client.artanis import Port

from nexus.core.dsl.nodes import Source
from nexus.core.runtime.context_store import ContextStore
from nexus.core.runtime.context_store_types import ContextId
from nexus.core.runtime.events import PipeToBus, SendEvent
from nexus.logging_utils import get_logger
from nexus.utils.exceptions import InternalFrameworkException

from .executor_communicator.common import NormalizedHttpPath
from .executor_communicator.http_server_lifecycle import start_uvicorn_server, stop_uvicorn_server

logger = get_logger(__name__)

MAX_EXCEPTION_DEPTH = 8

type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonPrimitive | list["JsonValue"] | dict[str, "JsonValue"]


@dataclass(frozen=True)
class PendingHttpResponse:
    status_code: int
    body: JsonValue


class PendingHttpResponseStore(ABC):
    @abstractmethod
    def create(self, ctx_id: ContextId) -> Future[PendingHttpResponse]:
        pass

    @abstractmethod
    def resolve(self, ctx_id: ContextId, response: PendingHttpResponse) -> bool:
        pass

    @abstractmethod
    def pop(self, ctx_id: ContextId) -> Future[PendingHttpResponse] | None:
        pass

    @abstractmethod
    def cancel_all(self) -> None:
        pass


class InMemoryPendingHttpResponseStore(PendingHttpResponseStore):
    _futures_by_ctx_id: dict[ContextId, Future[PendingHttpResponse]]
    _lock: threading.Lock

    def __init__(self) -> None:
        self._futures_by_ctx_id = {}
        self._lock = threading.Lock()

    def create(self, ctx_id: ContextId) -> Future[PendingHttpResponse]:
        with self._lock:
            if ctx_id in self._futures_by_ctx_id:
                raise InternalFrameworkException(f"Duplicate pending response future for context {ctx_id}.")
            future: Future[PendingHttpResponse] = Future()
            self._futures_by_ctx_id[ctx_id] = future
            return future

    def resolve(self, ctx_id: ContextId, response: PendingHttpResponse) -> bool:
        with self._lock:
            future = self._futures_by_ctx_id.pop(ctx_id, None)
        if future is None:
            return False
        if future.done():
            return False
        future.set_result(response)
        return True

    def pop(self, ctx_id: ContextId) -> Future[PendingHttpResponse] | None:
        with self._lock:
            return self._futures_by_ctx_id.pop(ctx_id, None)

    def cancel_all(self) -> None:
        with self._lock:
            pending_futures = tuple(self._futures_by_ctx_id.values())
            self._futures_by_ctx_id.clear()
        for pending_future in pending_futures:
            if not pending_future.done():
                pending_future.cancel()


def _exception_message(exc: BaseException) -> str:
    message = str(exc).strip()
    if message != "":
        return message
    return type(exc).__name__


def build_error_body(
    *,
    error_type: str,
    message: str,
    causes: tuple[tuple[str, str], ...] = (),
) -> dict[str, JsonValue]:
    return {
        "error": {
            "type": error_type,
            "message": message,
            "causes": [{"type": cause_type, "message": cause_message} for cause_type, cause_message in causes],
        }
    }


def exception_to_error_body(
    exception: BaseException,
    *,
    max_depth: int = MAX_EXCEPTION_DEPTH,
) -> dict[str, JsonValue]:
    causes: list[tuple[str, str]] = []
    current: BaseException | None = exception.__cause__ or exception.__context__
    depth = 0
    while current is not None and depth < max_depth:
        causes.append((type(current).__name__, _exception_message(current)))
        current = current.__cause__ or current.__context__
        depth += 1
    if current is not None:
        causes.append(
            (
                "TruncatedExceptionChain",
                f"Cause chain exceeded max depth={max_depth}.",
            )
        )

    return build_error_body(
        error_type=type(exception).__name__,
        message=_exception_message(exception),
        causes=tuple(causes),
    )


@dataclass(frozen=True)
class RestEntryPointRuntimeConfig[Model: BaseModel]:
    entry_point_id: str
    bind_host: str
    bind_port: Port
    path: NormalizedHttpPath
    user_data_model: type[Model]
    response_timeout: datetime.timedelta


@dataclass(frozen=True)
class RestEntryPointRuntimeDependencies[Model: BaseModel]:
    context_store: ContextStore
    pipe_to_bus: PipeToBus
    source: Source[Model]
    pending_response_store: PendingHttpResponseStore


@dataclass(frozen=True)
class RestEntryPointRuntimeStartup:
    thread_name: str
    startup_timeout: datetime.timedelta = datetime.timedelta(seconds=1)
    startup_poll_interval: datetime.timedelta = datetime.timedelta(milliseconds=25)
    startup_failure_join_timeout_seconds: float = 1.0
    shutdown_join_timeout_seconds: float = 1.0
    keep_alive_timeout_seconds: int = 5


@dataclass(frozen=True)
class RestEntryPointRuntime[Model: BaseModel]:
    server: uvicorn.Server
    thread: threading.Thread
    bound_port: Port
    config: RestEntryPointRuntimeConfig[Model]
    dependencies: RestEntryPointRuntimeDependencies[Model]
    shutdown_join_timeout_seconds: float

    @staticmethod
    def start[ModelT: BaseModel](
        *,
        config: RestEntryPointRuntimeConfig[ModelT],
        dependencies: RestEntryPointRuntimeDependencies[ModelT],
        startup: RestEntryPointRuntimeStartup,
    ) -> RestEntryPointRuntime[ModelT]:
        app = RestEntryPointRuntime._build_litestar_app(
            config=config,
            dependencies=dependencies,
        )

        server, server_thread, bound_port = start_uvicorn_server(
            app=app,
            host=config.bind_host,
            port=config.bind_port,
            thread_name=startup.thread_name,
            keep_alive_timeout_seconds=startup.keep_alive_timeout_seconds,
            startup_timeout=startup.startup_timeout,
            startup_poll_interval=startup.startup_poll_interval,
            startup_failure_join_timeout_seconds=startup.startup_failure_join_timeout_seconds,
            server_name="RestEntryPoint HTTP server",
        )

        logger.info(
            "RestEntryPoint listening on host=%s port=%s path=%r",
            config.bind_host,
            bound_port,
            config.path,
        )

        return RestEntryPointRuntime(
            server=server,
            thread=server_thread,
            bound_port=bound_port,
            config=config,
            dependencies=dependencies,
            shutdown_join_timeout_seconds=startup.shutdown_join_timeout_seconds,
        )

    @staticmethod
    def _build_litestar_app[ModelT: BaseModel](
        *,
        config: RestEntryPointRuntimeConfig[ModelT],
        dependencies: RestEntryPointRuntimeDependencies[ModelT],
    ) -> Litestar:
        @route(
            path=["/", "/{request_path:path}"],
            http_method=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
        )
        async def handle_request(request: Any, request_path: str | None = None) -> Response[JsonValue]:
            del request_path
            started_at = time.monotonic()
            status_code = 500

            try:
                status_code, body = await RestEntryPointRuntime._dispatch_request(
                    request=request,
                    config=config,
                    dependencies=dependencies,
                )
                return RestEntryPointRuntime._json_response(status_code=status_code, body=body)
            except Exception as exc:
                logger.exception(
                    "Unhandled RestEntryPoint request failure in entry_point=%s",
                    config.entry_point_id,
                    exc_info=exc,
                )
                status_code = 500
                return RestEntryPointRuntime._json_response(
                    status_code=status_code,
                    body=exception_to_error_body(exc),
                )
            finally:
                elapsed_ms = (time.monotonic() - started_at) * 1000.0
                logger.info(
                    "RestEntryPoint request: entry_point=%s method=%s path=%s status=%d duration_ms=%.2f",
                    config.entry_point_id,
                    request.method,
                    request.url.path,
                    status_code,
                    elapsed_ms,
                )

        return Litestar(route_handlers=[handle_request])

    @staticmethod
    async def _dispatch_request[ModelT: BaseModel](
        *,
        request: Any,
        config: RestEntryPointRuntimeConfig[ModelT],
        dependencies: RestEntryPointRuntimeDependencies[ModelT],
    ) -> tuple[int, JsonValue]:
        if request.url.path != config.path:
            return 404, build_error_body(error_type="NotFound", message="Route not found.")

        if request.method != "POST":
            return 405, build_error_body(error_type="MethodNotAllowed", message="Only POST is supported.")

        return await RestEntryPointRuntime._dispatch_post_request(
            request=request,
            config=config,
            dependencies=dependencies,
        )

    @staticmethod
    async def _dispatch_post_request[ModelT: BaseModel](
        *,
        request: Any,
        config: RestEntryPointRuntimeConfig[ModelT],
        dependencies: RestEntryPointRuntimeDependencies[ModelT],
    ) -> tuple[int, JsonValue]:
        content_length_header = request.headers.get("content-length")
        if content_length_header is None:
            return 411, build_error_body(error_type="LengthRequired", message="Content-Length header is required.")

        try:
            content_length = int(content_length_header)
        except ValueError:
            return 400, build_error_body(
                error_type="InvalidContentLength", message="Content-Length must be an integer."
            )

        try:
            body = await request.body()
        except Exception as exc:
            logger.warning("Failed reading RestEntryPoint request body.", exc_info=exc)
            return 400, build_error_body(error_type="InvalidRequestBody", message="Failed reading request body.")

        if content_length != len(body):
            return 400, build_error_body(
                error_type="InvalidContentLength",
                message="Content-Length does not match request body size.",
            )

        try:
            model = config.user_data_model.model_validate_json(body)
        except ValidationError as exc:
            return 400, build_error_body(
                error_type="ValidationError",
                message=f"Invalid request body: {exc}",
            )
        except Exception as exc:
            logger.warning("Failed parsing RestEntryPoint request body.", exc_info=exc)
            return 400, build_error_body(error_type="InvalidRequestBody", message="Request body is not valid JSON.")

        with dependencies.context_store.create_context() as context:
            ctx_id = context.id
        response_future = dependencies.pending_response_store.create(ctx_id)

        try:
            dependencies.pipe_to_bus.put(
                SendEvent(
                    ctx_id=ctx_id,
                    source=dependencies.source,
                    payload=model,
                )
            )
        except Exception:
            dependencies.pending_response_store.pop(ctx_id)
            raise

        try:
            pending_response = await asyncio.wait_for(
                asyncio.wrap_future(response_future),
                timeout=config.response_timeout.total_seconds(),
            )
            return pending_response.status_code, pending_response.body
        except TimeoutError:
            popped_future = dependencies.pending_response_store.pop(ctx_id)
            if popped_future is not None and not popped_future.done():
                popped_future.cancel()
            return 504, build_error_body(error_type="GatewayTimeout", message="Timed out waiting for task output.")
        except asyncio.CancelledError:
            popped_future = dependencies.pending_response_store.pop(ctx_id)
            if popped_future is not None and not popped_future.done():
                popped_future.cancel()
            return 503, build_error_body(
                error_type="ServiceUnavailable",
                message="Service is shutting down.",
            )

    @staticmethod
    def _json_response(*, status_code: int, body: JsonValue) -> Response[JsonValue]:
        return Response(
            content=body,
            status_code=status_code,
            media_type=MediaType.JSON,
        )

    def stop(self) -> None:
        stop_uvicorn_server(
            server=self.server,
            server_thread=self.thread,
            shutdown_join_timeout_seconds=self.shutdown_join_timeout_seconds,
            timeout_warning_message="RestEntryPoint HTTP server thread did not stop within timeout.",
            logger=logger,
        )
