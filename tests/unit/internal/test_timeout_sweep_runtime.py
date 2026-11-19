# pyright: basic

import datetime
from collections.abc import Callable
from typing import override

import pytest
from utils import wait_until

from nexus._internal.actors.executor_communicator.runtime_callbacks import CommunicatorErrorCallback
from nexus._internal.actors.executor_communicator.timeout_sweep_runtime import (
    TimeoutSweepRuntime,
    TimeoutSweepRuntimeConfig,
    TimeoutSweepRuntimeDependencies,
    TimeoutSweepRuntimeStartup,
)
from nexus.v1 import (
    ContextId,
    InMemoryPendingAsyncHttpRequestStore,
    InvalidContextIdException,
    NexusException,
    PendingAsyncHttpRequest,
    PendingAsyncHttpRequestStore,
    RemoteResponseTimeoutException,
    RequestId,
)

BAD_CTX = ContextId("ctx-bad")
SWEEP_LOGGER = "nexus._internal.actors.executor_communicator.timeout_sweep_runtime"


class FailingOnceStore(InMemoryPendingAsyncHttpRequestStore):
    """Store whose first `pop_expired` call fails."""

    def __init__(self) -> None:
        super().__init__()
        self.failed = False

    @override
    def pop_expired(self, *, now: datetime.datetime) -> tuple[PendingAsyncHttpRequest, ...]:
        if not self.failed:
            self.failed = True
            raise RuntimeError("store unavailable")
        return super().pop_expired(now=now)


def _expired_request(name: str) -> PendingAsyncHttpRequest:
    return PendingAsyncHttpRequest(
        request_id=RequestId(f"request-{name}"),
        ctx_id=ContextId(f"ctx-{name}"),
        expires_at=datetime.datetime.now(tz=datetime.UTC),
    )


def _start_sweep(
    store: PendingAsyncHttpRequestStore,
    emit_executor_error: Callable[[ContextId, NexusException], None],
) -> TimeoutSweepRuntime:
    return TimeoutSweepRuntime.start(
        config=TimeoutSweepRuntimeConfig(
            communicator_id="communicator",
            sweep_timeout=datetime.timedelta(milliseconds=10),
        ),
        dependencies=TimeoutSweepRuntimeDependencies(
            pending_request_store=store,
            error_callback=CommunicatorErrorCallback(emit_executor_error=emit_executor_error),
        ),
        startup=TimeoutSweepRuntimeStartup(thread_name="timeout-sweep-test"),
    )


def _sweep_errors(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.name == SWEEP_LOGGER]


def test_timeout_sweep_survives_failing_emit(caplog: pytest.LogCaptureFixture) -> None:
    emitted: list[tuple[ContextId, NexusException]] = []

    def emit_executor_error(ctx_id: ContextId, error: NexusException) -> None:
        if ctx_id == BAD_CTX:
            raise InvalidContextIdException(f"Context {ctx_id} not found")
        emitted.append((ctx_id, error))

    store = InMemoryPendingAsyncHttpRequestStore()
    # Registered before the sweep starts, so the first sweep pops all three as one batch, failing one first.
    for name in ("bad", "first", "second"):
        store.put(_expired_request(name))

    with caplog.at_level("ERROR"):
        runtime = _start_sweep(store, emit_executor_error)
        try:
            wait_until(lambda: len(emitted) == 2)
            store.put(_expired_request("late"))
            wait_until(lambda: len(emitted) == 3)
        finally:
            runtime.stop(join_timeout_seconds=1.0)

    assert [ctx_id for ctx_id, _ in emitted] == [ContextId("ctx-first"), ContextId("ctx-second"), ContextId("ctx-late")]
    assert all(isinstance(error, RemoteResponseTimeoutException) for _, error in emitted)
    assert _sweep_errors(caplog) == [
        "Failed to emit timeout for request request-bad (ctx=ctx-bad) in communicator=communicator."
    ]


def test_timeout_sweep_survives_failing_store(caplog: pytest.LogCaptureFixture) -> None:
    emitted: list[ContextId] = []
    store = FailingOnceStore()
    store.put(_expired_request("first"))

    with caplog.at_level("ERROR"):
        runtime = _start_sweep(store, lambda ctx_id, _: emitted.append(ctx_id))
        try:
            wait_until(lambda: len(emitted) == 1)
        finally:
            runtime.stop(join_timeout_seconds=1.0)

    assert emitted == [ContextId("ctx-first")]
    assert _sweep_errors(caplog) == ["Failed to collect expired requests in communicator=communicator."]
