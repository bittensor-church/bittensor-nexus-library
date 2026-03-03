from __future__ import annotations

import datetime
import threading
from dataclasses import dataclass

from nexus.utils.exceptions import RemoteResponseTimeoutException

from .common import timeout_seconds
from .pending_requests import PendingAsyncHttpRequestStore
from .runtime_callbacks import CommunicatorErrorCallback


@dataclass(frozen=True)
class TimeoutSweepRuntimeConfig:
    communicator_id: str
    sweep_timeout: datetime.timedelta


@dataclass(frozen=True)
class TimeoutSweepRuntimeDependencies:
    pending_request_store: PendingAsyncHttpRequestStore
    error_callback: CommunicatorErrorCallback


@dataclass(frozen=True)
class TimeoutSweepRuntimeStartup:
    thread_name: str


@dataclass(frozen=True)
class TimeoutSweepRuntime:
    """
    Background runtime that enforces request-level total processing deadlines.

    Purpose:
    - continuously scan the shared pending-request store for expired entries
    - remove expired requests from the store
    - emit `RemoteResponseTimeoutException` for each expired request via executor-failure callback

    How it works:
    - `start(...)` creates a dedicated daemon thread
    - that thread runs `_run_timeout_sweep_loop(...)`
    - each iteration computes `now`, pops all expired requests, emits timeout errors,
      then waits up to `sweep_timeout` (or exits earlier if stop is requested)
    """

    thread: threading.Thread
    stop_signal: threading.Event
    pending_request_store: PendingAsyncHttpRequestStore
    error_callback: CommunicatorErrorCallback
    sweep_timeout: datetime.timedelta

    @staticmethod
    def start(
        *,
        config: TimeoutSweepRuntimeConfig,
        dependencies: TimeoutSweepRuntimeDependencies,
        startup: TimeoutSweepRuntimeStartup,
    ) -> TimeoutSweepRuntime:
        """Start the timeout sweep thread and return the runtime handle."""

        stop_signal = threading.Event()
        timeout_thread = threading.Thread(
            target=TimeoutSweepRuntime._run_timeout_sweep_loop,
            kwargs={
                "stop_signal": stop_signal,
                "communicator_id": config.communicator_id,
                "sweep_timeout": config.sweep_timeout,
                "pending_request_store": dependencies.pending_request_store,
                "error_callback": dependencies.error_callback,
            },
            daemon=True,
            name=startup.thread_name,
        )
        timeout_thread.start()
        return TimeoutSweepRuntime(
            thread=timeout_thread,
            stop_signal=stop_signal,
            pending_request_store=dependencies.pending_request_store,
            error_callback=dependencies.error_callback,
            sweep_timeout=config.sweep_timeout,
        )

    @staticmethod
    def _run_timeout_sweep_loop(
        *,
        stop_signal: threading.Event,
        communicator_id: str,
        sweep_timeout: datetime.timedelta,
        pending_request_store: PendingAsyncHttpRequestStore,
        error_callback: CommunicatorErrorCallback,
    ) -> None:
        """Periodically expire overdue requests and emit timeout errors."""

        while not stop_signal.is_set():
            now = datetime.datetime.now(tz=datetime.UTC)
            expired_requests = pending_request_store.pop_expired(now=now)
            for request in expired_requests:
                error_callback.emit_executor_error(
                    request.ctx_id,
                    RemoteResponseTimeoutException(
                        "Timed out waiting for remote response "
                        f"for request {request.request_id} in communicator={communicator_id}."
                    ),
                )
            stop_signal.wait(timeout_seconds(sweep_timeout))

    def stop(self, *, join_timeout_seconds: float) -> None:
        """Request loop shutdown and wait up to `join_timeout_seconds` for thread exit."""

        self.stop_signal.set()
        self.thread.join(timeout=join_timeout_seconds)
