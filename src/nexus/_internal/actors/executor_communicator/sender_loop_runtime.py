from __future__ import annotations

import asyncio
import concurrent.futures
import datetime
import queue
import threading
import uuid
from dataclasses import dataclass

import httpx
from pydantic import AnyHttpUrl, BaseModel, TypeAdapter
from pylon_client.artanis import AsyncPylonClient, MtlsVerificationError
from pylon_client.artanis.v1 import Neuron

from nexus._internal.core.runtime.context_store_types import ContextId
from nexus._internal.logging_utils import get_logger
from nexus._internal.utils.exceptions import (
    AsyncHttpNeuronCommunicatorException,
    InternalFrameworkException,
    MinerMtlsVerificationException,
    NexusException,
    RemoteRequestFailedException,
    RemoteRequestRejectedException,
)

from .async_http_protocol import AsyncHttpNeuronRequestEnvelope, RequestId
from .common import NormalizedHttpPath, timeout_seconds
from .pending_requests import PendingAsyncHttpRequest, PendingAsyncHttpRequestStore
from .runtime_callbacks import CommunicatorErrorCallback

logger = get_logger(__name__)
_ANY_HTTP_URL_ADAPTER = TypeAdapter(AnyHttpUrl)


@dataclass(frozen=True)
class PendingSendRequest[InputModel: BaseModel]:
    request_id: RequestId
    ctx_id: ContextId
    target_url: AnyHttpUrl
    target_neuron: Neuron
    payload: InputModel


@dataclass(frozen=True)
class StopSenderLoopSignal:
    """Explicit sentinel used to stop a sender worker loop."""

    pass


STOP_SENDER_LOOP_SIGNAL = StopSenderLoopSignal()
type SenderLoopQueueItem[InputModel: BaseModel] = PendingSendRequest[InputModel] | StopSenderLoopSignal


@dataclass(frozen=True)
class SenderLoopRuntimeConfig[InputModel: BaseModel]:
    communicator_id: str
    queue_max_size: int
    queue_enqueue_timeout: datetime.timedelta
    send_timeout: datetime.timedelta
    max_in_flight: int
    total_processing_timeout: datetime.timedelta
    callback_base_url: AnyHttpUrl
    response_path: NormalizedHttpPath
    input_model: type[InputModel]
    pylon_client: AsyncPylonClient


@dataclass(frozen=True)
class SenderLoopRuntimeDependencies:
    pending_request_store: PendingAsyncHttpRequestStore
    error_callback: CommunicatorErrorCallback


@dataclass(frozen=True)
class SenderLoopRuntimeStartup:
    thread_name: str
    start_timeout: datetime.timedelta
    startup_failure_join_timeout_seconds: float


@dataclass(frozen=True)
class SenderLoopRuntime[InputModel: BaseModel]:
    """
    Background runtime responsible for outbound async HTTP dispatch.

    Purpose:
    - accept `dispatch(...)` calls from the actor thread
    - create and register pending request records with processing deadlines
    - enqueue send work into a dedicated asyncio loop
    - execute HTTP sends concurrently (up to `max_in_flight`)
    - convert enqueue/send failures into executor-failure callbacks

    How it works:
    - `start(...)` spins up a daemon thread with a private asyncio loop and queue
    - `dispatch(...)` records pending state and schedules a queue put into that loop
    - `_sender_loop_main(...)` creates a shared `httpx.AsyncClient` and worker tasks
    - each worker processes queue items until it receives `StopSenderLoopSignal`
    - `stop(...)` enqueues one stop signal per worker and joins the sender thread
    """

    thread: threading.Thread
    loop: asyncio.AbstractEventLoop
    queue: asyncio.Queue[SenderLoopQueueItem[InputModel]]
    communicator_id: str
    queue_enqueue_timeout: datetime.timedelta
    send_timeout: datetime.timedelta
    max_in_flight: int
    total_processing_timeout: datetime.timedelta
    callback_base_url: AnyHttpUrl
    response_path: NormalizedHttpPath
    input_model: type[InputModel]
    pending_request_store: PendingAsyncHttpRequestStore
    error_callback: CommunicatorErrorCallback
    pylon_client: AsyncPylonClient

    @staticmethod
    def start[InputModelT: BaseModel](
        *,
        config: SenderLoopRuntimeConfig[InputModelT],
        dependencies: SenderLoopRuntimeDependencies,
        startup: SenderLoopRuntimeStartup,
    ) -> SenderLoopRuntime[InputModelT]:
        """
        Start the sender thread + asyncio loop and return the runtime handle.

        Raises:
            InternalFrameworkException: If bootstrap times out or fails.

        """

        bootstrap_queue: queue.Queue[
            tuple[asyncio.AbstractEventLoop, asyncio.Queue[SenderLoopQueueItem[InputModelT]]] | Exception
        ] = queue.Queue(maxsize=1)
        sender_thread = threading.Thread(
            target=SenderLoopRuntime._run_sender_loop,
            kwargs={
                "bootstrap_queue": bootstrap_queue,
                "config": config,
                "dependencies": dependencies,
            },
            daemon=True,
            name=startup.thread_name,
        )
        sender_thread.start()

        try:
            bootstrap_result = bootstrap_queue.get(timeout=timeout_seconds(startup.start_timeout))
        except queue.Empty as exc:
            raise InternalFrameworkException(
                f"Async sender loop did not start within {startup.start_timeout!r}."
            ) from exc

        if isinstance(bootstrap_result, Exception):
            sender_thread.join(timeout=startup.startup_failure_join_timeout_seconds)
            raise InternalFrameworkException("Async sender loop failed during startup.") from bootstrap_result

        sender_loop, sender_queue = bootstrap_result
        return SenderLoopRuntime(
            thread=sender_thread,
            loop=sender_loop,
            queue=sender_queue,
            communicator_id=config.communicator_id,
            queue_enqueue_timeout=config.queue_enqueue_timeout,
            send_timeout=config.send_timeout,
            max_in_flight=config.max_in_flight,
            total_processing_timeout=config.total_processing_timeout,
            callback_base_url=config.callback_base_url,
            response_path=config.response_path,
            input_model=config.input_model,
            pending_request_store=dependencies.pending_request_store,
            error_callback=dependencies.error_callback,
            pylon_client=config.pylon_client,
        )

    @staticmethod
    def _run_sender_loop[InputModelT: BaseModel](
        *,
        bootstrap_queue: queue.Queue[
            tuple[asyncio.AbstractEventLoop, asyncio.Queue[SenderLoopQueueItem[InputModelT]]] | Exception
        ],
        config: SenderLoopRuntimeConfig[InputModelT],
        dependencies: SenderLoopRuntimeDependencies,
    ) -> None:
        try:
            sender_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(sender_loop)
            sender_queue: asyncio.Queue[SenderLoopQueueItem[InputModelT]] = asyncio.Queue(maxsize=config.queue_max_size)
            bootstrap_queue.put((sender_loop, sender_queue))
        except Exception as exc:
            bootstrap_queue.put(exc)
            return

        try:
            sender_loop.run_until_complete(
                SenderLoopRuntime._sender_loop_main(
                    sender_queue=sender_queue,
                    communicator_id=config.communicator_id,
                    send_timeout=config.send_timeout,
                    max_in_flight=config.max_in_flight,
                    callback_base_url=config.callback_base_url,
                    response_path=config.response_path,
                    input_model=config.input_model,
                    pending_request_store=dependencies.pending_request_store,
                    error_callback=dependencies.error_callback,
                    pylon_client=config.pylon_client,
                )
            )
        except Exception as exc:
            logger.exception(
                "Async sender loop crashed in communicator=%s.",
                config.communicator_id,
                exc_info=exc,
            )
        finally:
            try:
                sender_loop.run_until_complete(sender_loop.shutdown_asyncgens())
            except Exception as exc:
                logger.warning(
                    "Failed to shutdown async sender loop cleanly in communicator=%s.",
                    config.communicator_id,
                    exc_info=exc,
                )
            sender_loop.close()

    def dispatch(
        self,
        *,
        ctx_id: ContextId,
        target_url: AnyHttpUrl,
        target_neuron: Neuron,
        payload: InputModel,
    ) -> None:
        """
        Register and enqueue one outbound request.

        The request is added to the pending store before enqueueing so timeout sweep and
        callback processing can resolve it regardless of send outcome.
        """

        request_id = RequestId(str(uuid.uuid7()))
        expires_at = datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(
            seconds=timeout_seconds(self.total_processing_timeout)
        )
        self.pending_request_store.put(
            PendingAsyncHttpRequest(
                request_id=request_id,
                ctx_id=ctx_id,
                expires_at=expires_at,
            )
        )

        pending_send = PendingSendRequest(
            request_id=request_id,
            ctx_id=ctx_id,
            target_url=target_url,
            target_neuron=target_neuron,
            payload=payload,
        )
        self._enqueue_send_request(pending_send)

    def _enqueue_send_request(self, pending_send: PendingSendRequest[InputModel]) -> None:
        try:
            enqueue_future = asyncio.run_coroutine_threadsafe(self.queue.put(pending_send), self.loop)
        except RuntimeError:
            self._fail_pending_request(
                request_id=pending_send.request_id,
                fallback_ctx_id=pending_send.ctx_id,
                error=InternalFrameworkException("Sender loop is not running."),
            )
            return

        try:
            enqueue_future.result(timeout=timeout_seconds(self.queue_enqueue_timeout))
        except concurrent.futures.TimeoutError:
            enqueue_future.cancel()
            self._fail_pending_request(
                request_id=pending_send.request_id,
                fallback_ctx_id=pending_send.ctx_id,
                error=RemoteRequestFailedException(
                    f"Timed out enqueueing request {pending_send.request_id} for async send."
                ),
            )
        except Exception as exc:
            self._fail_pending_request(
                request_id=pending_send.request_id,
                fallback_ctx_id=pending_send.ctx_id,
                error=InternalFrameworkException(
                    f"Failed enqueueing request {pending_send.request_id} for async send: {exc!r}"
                ),
            )

    @staticmethod
    async def _sender_loop_main[InputModelT: BaseModel](
        *,
        sender_queue: asyncio.Queue[SenderLoopQueueItem[InputModelT]],
        communicator_id: str,
        send_timeout: datetime.timedelta,
        max_in_flight: int,
        callback_base_url: AnyHttpUrl,
        response_path: NormalizedHttpPath,
        input_model: type[InputModelT],
        pending_request_store: PendingAsyncHttpRequestStore,
        error_callback: CommunicatorErrorCallback,
        pylon_client: AsyncPylonClient,
    ) -> None:
        """Run sender workers until all receive a stop signal."""

        async with pylon_client:
            workers = [
                asyncio.create_task(
                    SenderLoopRuntime._sender_worker(
                        sender_queue=sender_queue,
                        communicator_id=communicator_id,
                        send_timeout=send_timeout,
                        callback_base_url=callback_base_url,
                        response_path=response_path,
                        input_model=input_model,
                        pending_request_store=pending_request_store,
                        error_callback=error_callback,
                        pylon_client=pylon_client,
                    ),
                    name=f"AsyncHttpSenderWorker-{communicator_id}-{index}",
                )
                for index in range(max_in_flight)
            ]
            try:
                await asyncio.gather(*workers)
            finally:
                for worker in workers:
                    if not worker.done():
                        worker.cancel()
                await asyncio.gather(*workers, return_exceptions=True)

    @staticmethod
    async def _sender_worker[InputModelT: BaseModel](
        *,
        sender_queue: asyncio.Queue[SenderLoopQueueItem[InputModelT]],
        communicator_id: str,
        send_timeout: datetime.timedelta,
        callback_base_url: AnyHttpUrl,
        response_path: NormalizedHttpPath,
        input_model: type[InputModelT],
        pending_request_store: PendingAsyncHttpRequestStore,
        error_callback: CommunicatorErrorCallback,
        pylon_client: AsyncPylonClient,
    ) -> None:
        """Consume queued sends until a stop signal is received."""

        while True:
            queue_item = await sender_queue.get()
            if isinstance(queue_item, StopSenderLoopSignal):
                return
            pending_send = queue_item

            try:
                await SenderLoopRuntime._send_request(
                    pending_send=pending_send,
                    send_timeout=send_timeout,
                    callback_base_url=callback_base_url,
                    response_path=response_path,
                    input_model=input_model,
                    pylon_client=pylon_client,
                )
            except AsyncHttpNeuronCommunicatorException as exc:
                SenderLoopRuntime._fail_pending_request_static(
                    pending_request_store=pending_request_store,
                    error_callback=error_callback,
                    communicator_id=communicator_id,
                    request_id=pending_send.request_id,
                    fallback_ctx_id=pending_send.ctx_id,
                    error=exc,
                )
            except Exception as exc:
                SenderLoopRuntime._fail_pending_request_static(
                    pending_request_store=pending_request_store,
                    error_callback=error_callback,
                    communicator_id=communicator_id,
                    request_id=pending_send.request_id,
                    fallback_ctx_id=pending_send.ctx_id,
                    error=RemoteRequestFailedException(
                        f"Unexpected failure while sending request {pending_send.request_id}: {exc!r}"
                    ),
                )

    @staticmethod
    async def _send_request[InputModelT: BaseModel](
        *,
        pending_send: PendingSendRequest[InputModelT],
        send_timeout: datetime.timedelta,
        callback_base_url: AnyHttpUrl,
        response_path: NormalizedHttpPath,
        input_model: type[InputModelT],
        pylon_client: AsyncPylonClient,
    ) -> None:
        body = (
            AsyncHttpNeuronRequestEnvelope(
                request_id=pending_send.request_id,
                callback_url=SenderLoopRuntime._callback_url(
                    callback_base_url=callback_base_url,
                    response_path=response_path,
                ),
                input=input_model.model_validate(pending_send.payload).model_dump(mode="json"),
            )
            .model_dump_json()
            .encode("utf-8")
        )

        extra_headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Length": str(len(body)),
            "X-Nexus-Request-Id": str(pending_send.request_id),
        }

        timeout = timeout_seconds(send_timeout)
        path = str(pending_send.target_url.path)

        try:
            async with pylon_client.get_neuron_client(pending_send.target_neuron, timeout=timeout) as client:
                resp = await client.post(
                    path,
                    content=body,
                    headers=extra_headers,
                    timeout=timeout,
                )
                if resp.status_code < 200 or resp.status_code >= 300:
                    raise RemoteRequestRejectedException(
                        f"Remote service rejected request {pending_send.request_id} with HTTP status={resp.status_code}"
                    )
        except MtlsVerificationError as exc:
            raise MinerMtlsVerificationException(str(exc)) from exc
        except httpx.TimeoutException as exc:
            raise RemoteRequestFailedException(
                f"Timeout while sending request {pending_send.request_id} to {pending_send.target_url!r}"
            ) from exc
        except httpx.RequestError as exc:
            raise RemoteRequestFailedException(
                f"Network error while sending request {pending_send.request_id} to {pending_send.target_url!r}: {exc!r}"
            ) from exc

    @staticmethod
    def _callback_url(
        *,
        callback_base_url: AnyHttpUrl,
        response_path: NormalizedHttpPath,
    ) -> AnyHttpUrl:
        callback_base = str(callback_base_url).rstrip("/")
        callback_url = f"{callback_base}{response_path}"
        return _ANY_HTTP_URL_ADAPTER.validate_python(callback_url)

    def _fail_pending_request(
        self,
        *,
        request_id: RequestId,
        fallback_ctx_id: ContextId,
        error: NexusException,
    ) -> None:
        self._fail_pending_request_static(
            pending_request_store=self.pending_request_store,
            error_callback=self.error_callback,
            communicator_id=self.communicator_id,
            request_id=request_id,
            fallback_ctx_id=fallback_ctx_id,
            error=error,
        )

    @staticmethod
    def _fail_pending_request_static(
        *,
        pending_request_store: PendingAsyncHttpRequestStore,
        error_callback: CommunicatorErrorCallback,
        communicator_id: str,
        request_id: RequestId,
        fallback_ctx_id: ContextId,
        error: NexusException,
    ) -> None:
        pending_request = pending_request_store.pop(request_id)
        if pending_request is not None:
            error_callback.emit_executor_error(pending_request.ctx_id, error)
            return

        logger.warning(
            "Failed request %s in communicator=%s had no pending record (already completed/expired). Fallback ctx=%s",
            request_id,
            communicator_id,
            fallback_ctx_id,
        )

    def stop(
        self,
        *,
        enqueue_timeout: datetime.timedelta,
        join_timeout_seconds: float,
    ) -> None:
        """Request sender worker shutdown and wait for the sender thread to exit."""

        try:
            stop_sender_future = asyncio.run_coroutine_threadsafe(
                self._enqueue_stop_signals(),
                self.loop,
            )
            stop_sender_future.result(timeout=timeout_seconds(enqueue_timeout))
        except Exception as exc:
            logger.warning("Failed to signal sender loop shutdown cleanly.", exc_info=exc)
        self.thread.join(timeout=join_timeout_seconds)

    async def _enqueue_stop_signals(self) -> None:
        for _ in range(self.max_in_flight):
            await self.queue.put(STOP_SENDER_LOOP_SIGNAL)
