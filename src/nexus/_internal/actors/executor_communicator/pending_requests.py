import datetime
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import override

from nexus._internal.core.runtime.context_store_types import ContextId
from nexus._internal.utils.exceptions import InternalFrameworkException

from .async_http_protocol import RequestId


@dataclass(frozen=True)
class PendingAsyncHttpRequest:
    """Request metadata kept while waiting for asynchronous callback delivery."""

    request_id: RequestId
    ctx_id: ContextId
    expires_at: datetime.datetime


class PendingAsyncHttpRequestStore(ABC):
    """
    Storage for in-flight async HTTP requests keyed by ``request_id``.

    The store is shared by multiple runtimes:
    - sender runtime registers requests via ``put``
    - callback runtime resolves them via ``pop``
    - timeout runtime removes overdue entries via ``pop_expired``

    Implementations must be safe for concurrent access from multiple threads.
    """

    @abstractmethod
    def put(self, request: PendingAsyncHttpRequest) -> None:
        """Register a new pending request. Must fail if ``request_id`` already exists."""

        pass

    @abstractmethod
    def pop(self, request_id: RequestId) -> PendingAsyncHttpRequest | None:
        """
        Remove and return the pending request for ``request_id``.

        Returns ``None`` when there is no matching entry (already completed, expired, or unknown).
        """

        pass

    @abstractmethod
    def pop_expired(self, *, now: datetime.datetime) -> tuple[PendingAsyncHttpRequest, ...]:
        """
        Remove and return all requests with ``expires_at <= now``.

        Returned items are no longer present in the store.
        """

        pass


class InMemoryPendingAsyncHttpRequestStore(PendingAsyncHttpRequestStore):
    _requests_by_id: dict[RequestId, PendingAsyncHttpRequest]
    _lock: threading.Lock

    def __init__(self) -> None:
        self._requests_by_id = {}
        self._lock = threading.Lock()

    @override
    def put(self, request: PendingAsyncHttpRequest) -> None:
        with self._lock:
            if request.request_id in self._requests_by_id:
                raise InternalFrameworkException(f"Duplicate pending request id: {request.request_id}")
            self._requests_by_id[request.request_id] = request

    @override
    def pop(self, request_id: RequestId) -> PendingAsyncHttpRequest | None:
        with self._lock:
            return self._requests_by_id.pop(request_id, None)

    @override
    def pop_expired(self, *, now: datetime.datetime) -> tuple[PendingAsyncHttpRequest, ...]:
        with self._lock:
            expired = [request for request in self._requests_by_id.values() if request.expires_at <= now]
            for request in expired:
                self._requests_by_id.pop(request.request_id, None)
            return tuple(expired)
