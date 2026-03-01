from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pydantic import BaseModel

from nexus.core.runtime.context_store_types import ContextId
from nexus.utils.exceptions import NexusException


@dataclass(frozen=True)
class CommunicatorProcessedCallback[Output: BaseModel]:
    """
    Callback used by runtimes that can emit successfully processed payloads.

    Called when a pending request completed successfully and produced a validated response payload.
    Receives the request context id and the validated model instance that should be
    emitted on the communicator `processed` output.
    """

    emit_processed: Callable[[ContextId, Output], None]


@dataclass(frozen=True)
class CommunicatorErrorCallback:
    """
    Callback used by runtimes to report failures for pending requests.

    Called when a pending request cannot be completed successfully (send failure, timeout,
    invalid callback body, remote execution error, etc.). Receives the request context id
    and a `NexusException` that should be emitted on the communicator `error` output.
    """

    emit_error: Callable[[ContextId, NexusException], None]
