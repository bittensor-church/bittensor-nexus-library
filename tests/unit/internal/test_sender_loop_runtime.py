# pyright: basic

import datetime

import pytest

from nexus._internal.actors.executor_communicator.runtime_callbacks import CommunicatorErrorCallback
from nexus._internal.actors.executor_communicator.sender_loop_runtime import SenderLoopRuntime
from nexus.v1 import (
    ContextId,
    InMemoryPendingAsyncHttpRequestStore,
    InvalidContextIdException,
    NexusException,
    PendingAsyncHttpRequest,
    RemoteRequestFailedException,
    RequestId,
)


def test_failing_pending_request_survives_failing_emit(caplog: pytest.LogCaptureFixture) -> None:
    def emit_executor_error(ctx_id: ContextId, _: NexusException) -> None:
        raise InvalidContextIdException(f"Context {ctx_id} not found")

    request_id = RequestId("request-bad")
    store = InMemoryPendingAsyncHttpRequestStore()
    store.put(
        PendingAsyncHttpRequest(
            request_id=request_id,
            ctx_id=ContextId("ctx-bad"),
            expires_at=datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(minutes=1),
        )
    )

    with caplog.at_level("ERROR"):
        SenderLoopRuntime._fail_pending_request_static(
            pending_request_store=store,
            error_callback=CommunicatorErrorCallback(emit_executor_error=emit_executor_error),
            communicator_id="communicator",
            request_id=request_id,
            fallback_ctx_id=ContextId("ctx-bad"),
            error=RemoteRequestFailedException("send failed"),
        )

    assert store.pop(request_id) is None
    assert [record.getMessage() for record in caplog.records] == [
        "Failed to emit error for request request-bad (ctx=ctx-bad) in communicator=communicator."
    ]
