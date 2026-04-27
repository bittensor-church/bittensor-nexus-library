"""Shared lifecycle helpers for thread-hosted Uvicorn HTTP servers."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from logging import Logger
from typing import Any, cast

import uvicorn
from pylon_client.artanis import Port

from nexus._internal.utils.exceptions import InternalFrameworkException


def start_uvicorn_server(
    *,
    app: Any,
    host: str,
    port: Port,
    thread_name: str,
    keep_alive_timeout_seconds: int,
    startup_timeout: timedelta,
    startup_poll_interval: timedelta,
    startup_failure_join_timeout_seconds: float,
    server_name: str,
) -> tuple[uvicorn.Server, threading.Thread, Port]:
    uvicorn_config = uvicorn.Config(
        app=app,
        host=host,
        port=int(port),
        ws="none",
        access_log=False,
        timeout_keep_alive=keep_alive_timeout_seconds,
        server_header=False,
        date_header=False,
    )
    server = uvicorn.Server(uvicorn_config)
    server_thread = threading.Thread(
        target=server.run,
        daemon=True,
        name=thread_name,
    )
    server_thread.start()

    try:
        bound_port = await_uvicorn_server_startup(
            server=server,
            server_thread=server_thread,
            startup_timeout=startup_timeout,
            startup_poll_interval=startup_poll_interval,
            server_name=server_name,
        )
    except Exception:
        server.should_exit = True
        server_thread.join(timeout=startup_failure_join_timeout_seconds)
        raise

    return server, server_thread, bound_port


def await_uvicorn_server_startup(
    *,
    server: uvicorn.Server,
    server_thread: threading.Thread,
    startup_timeout: timedelta,
    startup_poll_interval: timedelta,
    server_name: str,
) -> Port:
    deadline = time.monotonic() + startup_timeout.total_seconds()
    poll_seconds = startup_poll_interval.total_seconds()

    while time.monotonic() < deadline:
        if not server_thread.is_alive():
            raise InternalFrameworkException(f"{server_name} thread exited before startup completed.")

        if server.started:
            bound_port = resolve_bound_port(server)
            if bound_port is not None:
                return bound_port

        time.sleep(poll_seconds)

    raise InternalFrameworkException(f"{server_name} did not report startup within {startup_timeout!r}.")


def resolve_bound_port(server: uvicorn.Server) -> Port | None:
    for asgi_server in server.servers or []:
        for socket in asgi_server.sockets:
            sockaddr: Any = socket.getsockname()
            maybe_port = extract_socket_port(sockaddr)
            if maybe_port is not None:
                return Port(maybe_port)
    return None


def extract_socket_port(sockaddr: Any) -> int | None:
    if not isinstance(sockaddr, tuple):
        return None

    sockaddr_items = cast(tuple[Any, ...], sockaddr)
    if len(sockaddr_items) < 2:
        return None

    maybe_port = sockaddr_items[1]
    if isinstance(maybe_port, int):
        return maybe_port
    return None


def stop_uvicorn_server(
    *,
    server: uvicorn.Server | None,
    server_thread: threading.Thread | None,
    shutdown_join_timeout_seconds: float,
    timeout_warning_message: str,
    logger: Logger,
) -> None:
    if server is not None:
        server.should_exit = True
    if server_thread is not None:
        server_thread.join(timeout=shutdown_join_timeout_seconds)
        if server_thread.is_alive():
            logger.warning(timeout_warning_message)
