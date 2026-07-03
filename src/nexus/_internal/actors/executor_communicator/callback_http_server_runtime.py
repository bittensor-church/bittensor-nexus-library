"""
Callback HTTP server runtime for `AsyncHttpNeuronCommunicator`.

This module owns the small web server that receives asynchronous callback responses
from remote executors (neurons). It exposes a single POST endpoint, validates the
callback envelope, resolves the corresponding pending request, and emits either:
- `processed` callbacks for successfully parsed output payloads, or
- executor-failure callbacks on `processed` for remote execution and response-shape failures.
"""

from __future__ import annotations

import datetime
import threading
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from typing import Any

import uvicorn
from litestar import Litestar, post
from litestar.enums import MediaType
from litestar.response import Response
from pydantic import BaseModel, ValidationError
from pylon_client.artanis import Port

from nexus._internal.logging_utils import get_logger, host_friendly_logging_config
from nexus._internal.utils.exceptions import (
    RemoteExecutionException,
    ResponseInvalidException,
    ResponseValidationException,
)

from .async_http_protocol import AsyncHttpNeuronResponseEnvelope
from .common import NormalizedHttpPath
from .http_server_lifecycle import start_uvicorn_server, stop_uvicorn_server
from .pending_requests import PendingAsyncHttpRequestStore
from .runtime_callbacks import CommunicatorErrorCallback, CommunicatorProcessedCallback

logger = get_logger(__name__)


@dataclass(frozen=True)
class CallbackHttpServerRuntimeConfig[OutputModel: BaseModel]:
    """Configuration for binding and callback payload decoding."""

    communicator_id: str
    bind_host: IPv4Address | IPv6Address
    bind_port: Port
    response_path: NormalizedHttpPath
    output_model: type[OutputModel]


@dataclass(frozen=True)
class CallbackHttpServerRuntimeDependencies[OutputModel: BaseModel]:
    """Runtime dependencies used while processing callback requests."""

    pending_request_store: PendingAsyncHttpRequestStore
    processed_callback: CommunicatorProcessedCallback[OutputModel]
    error_callback: CommunicatorErrorCallback


@dataclass(frozen=True)
class CallbackHttpServerRuntimeStartup:
    """Startup and shutdown tuning for the callback server thread."""

    thread_name: str
    start_timeout: datetime.timedelta = datetime.timedelta(seconds=1)
    startup_poll_interval: datetime.timedelta = datetime.timedelta(milliseconds=25)
    startup_failure_join_timeout_seconds: float = 1.0
    shutdown_join_timeout_seconds: float = 1.0
    keep_alive_timeout_seconds: int = 5


@dataclass(frozen=True)
class CallbackHttpServerRuntime[OutputModel: BaseModel]:
    """
    Running callback HTTP server state.

    The runtime is intentionally thin: it starts/stops an embedded Uvicorn server and
    keeps the callback-processing dependencies grouped with the server metadata.
    """

    server: uvicorn.Server
    thread: threading.Thread
    bound_port: Port
    communicator_id: str
    response_path: NormalizedHttpPath
    output_model: type[OutputModel]
    pending_request_store: PendingAsyncHttpRequestStore
    processed_callback: CommunicatorProcessedCallback[OutputModel]
    error_callback: CommunicatorErrorCallback
    shutdown_join_timeout_seconds: float

    @staticmethod
    def start[OutputModelT: BaseModel](
        *,
        config: CallbackHttpServerRuntimeConfig[OutputModelT],
        dependencies: CallbackHttpServerRuntimeDependencies[OutputModelT],
        startup: CallbackHttpServerRuntimeStartup,
    ) -> CallbackHttpServerRuntime[OutputModelT]:
        """
        Start the callback HTTP server in a background thread.

        Returns:
            A running `CallbackHttpServerRuntime` with the effective bound port.

        """
        app = CallbackHttpServerRuntime._build_litestar_app(
            communicator_id=config.communicator_id,
            response_path=config.response_path,
            output_model=config.output_model,
            pending_request_store=dependencies.pending_request_store,
            processed_callback=dependencies.processed_callback,
            error_callback=dependencies.error_callback,
        )

        server, server_thread, bound_port = start_uvicorn_server(
            app=app,
            host=str(config.bind_host),
            port=config.bind_port,
            thread_name=startup.thread_name,
            keep_alive_timeout_seconds=startup.keep_alive_timeout_seconds,
            startup_timeout=startup.start_timeout,
            startup_poll_interval=startup.startup_poll_interval,
            startup_failure_join_timeout_seconds=startup.startup_failure_join_timeout_seconds,
            server_name="Callback HTTP server",
        )

        return CallbackHttpServerRuntime(
            server=server,
            thread=server_thread,
            bound_port=bound_port,
            communicator_id=config.communicator_id,
            response_path=config.response_path,
            output_model=config.output_model,
            pending_request_store=dependencies.pending_request_store,
            processed_callback=dependencies.processed_callback,
            error_callback=dependencies.error_callback,
            shutdown_join_timeout_seconds=startup.shutdown_join_timeout_seconds,
        )

    @staticmethod
    def _build_litestar_app[OutputModelT: BaseModel](
        *,
        communicator_id: str,
        response_path: NormalizedHttpPath,
        output_model: type[OutputModelT],
        pending_request_store: PendingAsyncHttpRequestStore,
        processed_callback: CommunicatorProcessedCallback[OutputModelT],
        error_callback: CommunicatorErrorCallback,
    ) -> Any:
        """
        Build the Litestar app serving the callback endpoint.

        The generated app exposes exactly one route: `POST {response_path}`.
        """

        @post(path=response_path)
        async def callback(request: Any) -> Response[str]:
            try:
                body = await request.body()
            except Exception as exc:
                logger.warning("Failed reading communicator callback request body.", exc_info=exc)
                return Response(
                    content="Failed reading request body\n",
                    status_code=400,
                    media_type=MediaType.TEXT,
                )

            try:
                status_code, response_body = CallbackHttpServerRuntime._process_callback(
                    body=body,
                    communicator_id=communicator_id,
                    output_model=output_model,
                    pending_request_store=pending_request_store,
                    processed_callback=processed_callback,
                    error_callback=error_callback,
                )
                return Response(
                    content=response_body,
                    status_code=status_code,
                    media_type=MediaType.TEXT,
                )
            except Exception as exc:
                logger.exception(
                    "Unhandled callback processing error in communicator=%s", communicator_id, exc_info=exc
                )
                return Response(content="Internal callback error\n", status_code=500, media_type=MediaType.TEXT)

        return Litestar(route_handlers=[callback], logging_config=host_friendly_logging_config())

    @staticmethod
    def _process_callback[OutputModelT: BaseModel](
        *,
        body: bytes,
        communicator_id: str,
        output_model: type[OutputModelT],
        pending_request_store: PendingAsyncHttpRequestStore,
        processed_callback: CommunicatorProcessedCallback[OutputModelT],
        error_callback: CommunicatorErrorCallback,
    ) -> tuple[int, str]:
        """
        Parse and handle one callback request body.

        Returns:
            `(status_code, response_body)` to be returned by the HTTP endpoint.

        Behavior:
            - Invalid envelope JSON -> `400`
            - Unknown request id -> `404`
            - Remote-side error/invalid output -> emit executor failure on `processed`, return `202`
            - Valid output payload -> emit processed output, return `202`

        """
        try:
            envelope = AsyncHttpNeuronResponseEnvelope.model_validate_json(body)
        except ValidationError as exc:
            return 400, f"Invalid callback body: {exc}\n"
        except Exception as exc:
            logger.warning("Failed parsing communicator callback body.", exc_info=exc)
            return 400, "Invalid callback body\n"

        pending_request = pending_request_store.pop(envelope.request_id)
        if pending_request is None:
            logger.warning(
                "Unknown callback request_id=%s in communicator=%s; dropping callback.",
                envelope.request_id,
                communicator_id,
            )
            return 404, "Unknown request_id\n"

        if envelope.error is not None:
            error_callback.emit_executor_error(
                pending_request.ctx_id,
                RemoteExecutionException(f"Remote returned error for request {envelope.request_id}: {envelope.error}"),
            )
            return 202, "accepted\n"

        if envelope.output is None:
            error_callback.emit_executor_error(
                pending_request.ctx_id,
                ResponseInvalidException(f"Remote response for request {envelope.request_id} did not include output."),
            )
            return 202, "accepted\n"

        try:
            parsed_output = output_model.model_validate(envelope.output)
        except ValidationError as exc:
            error_callback.emit_executor_error(
                pending_request.ctx_id,
                ResponseValidationException(
                    f"Remote response validation failed for request {envelope.request_id}: {exc}"
                ),
            )
            return 400, "Invalid callback payload\n"

        processed_callback.emit_processed(pending_request.ctx_id, parsed_output)
        return 202, "accepted\n"

    def stop(self) -> None:
        """Stop the callback server and join the background server thread."""
        stop_uvicorn_server(
            server=self.server,
            server_thread=self.thread,
            shutdown_join_timeout_seconds=self.shutdown_join_timeout_seconds,
            timeout_warning_message="Callback HTTP server thread did not stop within timeout.",
            logger=logger,
        )
