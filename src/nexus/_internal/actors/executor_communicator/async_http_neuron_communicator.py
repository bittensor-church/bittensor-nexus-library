from __future__ import annotations

import datetime
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from typing import override
from urllib.parse import urlparse

from pydantic import AnyHttpUrl, BaseModel, TypeAdapter
from pylon_client.artanis import Port
from pylon_client.artanis.v1 import AxonProtocol, Neuron

from nexus._internal.actors.neuron_router import Routed
from nexus._internal.actors.pylon_client_provider import DEFAULT_ASYNC_PYLON_CLIENT_PROVIDER, AsyncPylonClientProvider
from nexus._internal.core.runtime.actor import Actor, ActorBuilder
from nexus._internal.core.runtime.context_store import Context, ContextStore
from nexus._internal.core.runtime.events import MessagesToSend, PipeToBus, ReceiveEvent
from nexus._internal.logging_utils import get_logger
from nexus._internal.utils.exceptions import (
    ActorMisconfiguredException,
    InternalFrameworkException,
    NeuronAddressInvalidException,
    NexusException,
    RemoteRequestFailedException,
    UnsupportedAxonProtocolException,
)

from .base_communicator import CommunicatorActor, ExecutorCommunicator
from .callback_http_server_runtime import (
    CallbackHttpServerRuntime,
    CallbackHttpServerRuntimeConfig,
    CallbackHttpServerRuntimeDependencies,
    CallbackHttpServerRuntimeStartup,
)
from .common import NormalizedHttpPath, format_host_for_url, normalize_http_path, validate_positive_timeout
from .pending_requests import InMemoryPendingAsyncHttpRequestStore, PendingAsyncHttpRequestStore
from .runtime_callbacks import CommunicatorErrorCallback, CommunicatorProcessedCallback
from .sender_loop_runtime import (
    SenderLoopRuntime,
    SenderLoopRuntimeConfig,
    SenderLoopRuntimeDependencies,
    SenderLoopRuntimeStartup,
)
from .timeout_sweep_runtime import (
    TimeoutSweepRuntime,
    TimeoutSweepRuntimeConfig,
    TimeoutSweepRuntimeDependencies,
    TimeoutSweepRuntimeStartup,
)

logger = get_logger(__name__)
_ANY_HTTP_URL_ADAPTER = TypeAdapter(AnyHttpUrl)


@dataclass(frozen=True)
class HttpBindEndpoint:
    host: IPv4Address | IPv6Address
    port: Port


class AsyncHttpNeuronCommunicator[InputModel: BaseModel, OutputModel: BaseModel](
    ExecutorCommunicator[InputModel, OutputModel], ActorBuilder
):
    """
    ExecutorCommunicator that forwards routed input payloads to a target neuron's HTTP axon
    and asynchronously resolves responses via a local callback endpoint.
    Response to the request is required to arrive relatively fast, as it only serves as an acknowledgement signal.
    The actual result is expected to be delivered to the callback endpoint at a later time.
    For each input, it serializes the payload as JSON, sends it to the neuron's address on
    `target_path`, and stores a pending request keyed by request id. The communicator binds
    a callback HTTP server on `callback_bind_ip:callback_port` and advertises callbacks
    using `callback_base_url + callback_path`.

    sink input: routed request payloads
    source processed: successful results, executor-side failures (timeout, invalid payload, rejection)
    source error: internal/framework failures (e.g. misconfiguration, invalid target address)
    """

    target_path: NormalizedHttpPath
    send_timeout: datetime.timedelta
    total_processing_timeout: datetime.timedelta
    max_in_flight: int
    callback_bind_ip: IPv4Address
    callback_port: Port
    response_path: NormalizedHttpPath
    callback_base_url: AnyHttpUrl
    pending_request_store: PendingAsyncHttpRequestStore
    async_pylon_client_provider: AsyncPylonClientProvider

    def __init__(
        self,
        _id: str,
        *,
        target_path: str,
        send_timeout: datetime.timedelta,
        total_processing_timeout: datetime.timedelta,
        max_in_flight: int = 16,
        callback_bind_ip: IPv4Address | None = None,
        callback_port: Port,
        callback_path: str,
        callback_base_url: AnyHttpUrl | str,
        input_model: type[InputModel],
        output_model: type[OutputModel],
        pending_request_store: PendingAsyncHttpRequestStore | None = None,
        async_pylon_client_provider: AsyncPylonClientProvider | None = None,
    ) -> None:
        """
        Initialize an async HTTP communicator instance.

        mTLS is enabled by setting the ``VALIDATOR_MTLS_CERT_PATH`` and
        ``VALIDATOR_MTLS_KEY_PATH`` environment variables. Without them, requests use plain HTTP.

        Args:
            _id: Unique node/actor identifier used in runtime wiring and logging.
            target_path: HTTP path on the target neuron where input requests are sent.
                The value is normalized to a canonical HTTP path.
            send_timeout: Maximum duration for the outbound HTTP request to the neuron.
            total_processing_timeout: End-to-end deadline for a request, from dispatch
                until a callback is received. Once exceeded, the request is timed out.
            max_in_flight: Maximum number of concurrent requests being processed by this
                communicator instance.
            callback_bind_ip: Local host for binding the callback HTTP server that
                receives processed responses.
            callback_port: Local port for binding the callback HTTP server that
                receives processed responses.
            callback_path: Callback path exposed by the local callback HTTP server.
                The value is normalized to a canonical HTTP path.
            callback_base_url: Public base URL used when advertising callback endpoints
                to remote neurons. Must be an HTTP(S) URL without query or fragment.
            input_model: Pydantic model used to validate and serialize outbound
                request payloads.
            output_model: Pydantic model used to validate callback response payloads.
            pending_request_store: Backing store for in-flight request state. When
                omitted, the default store is created automatically.
            async_pylon_client_provider: Provider for the async Pylon client used to
                send requests to miners. Defaults to ``EnvAsyncPylonClientProvider``.

        Raises:
            ActorMisconfiguredException: If timeouts are non-positive, max_in_flight
                is not positive, response_bind.port is outside [0, 65535], or
                callback_base_url contains query/fragment components.

        """
        super().__init__(_id, input_model, output_model)
        validate_positive_timeout(timeout=send_timeout, parameter_name="send_timeout")
        validate_positive_timeout(
            timeout=total_processing_timeout,
            parameter_name="total_processing_timeout",
        )
        if max_in_flight <= 0:
            raise ActorMisconfiguredException("max_in_flight must be > 0")
        if not (0 <= int(callback_port) <= 65535):
            raise ActorMisconfiguredException("callback_port must be in [0, 65535]")
        parsed_callback_base_url = _ANY_HTTP_URL_ADAPTER.validate_python(callback_base_url)
        parsed_callback_base_url_parts = urlparse(str(parsed_callback_base_url))
        if parsed_callback_base_url_parts.query != "" or parsed_callback_base_url_parts.fragment != "":
            raise ActorMisconfiguredException("callback_base_url must not include query parameters or fragments.")
        self.target_path = normalize_http_path(target_path)
        self.send_timeout = send_timeout
        self.total_processing_timeout = total_processing_timeout
        self.max_in_flight = max_in_flight
        self.callback_bind_ip = callback_bind_ip or IPv4Address("0.0.0.0")
        self.callback_port = callback_port
        self.response_path = normalize_http_path(callback_path)
        self.callback_base_url = parsed_callback_base_url
        self.pending_request_store = pending_request_store or InMemoryPendingAsyncHttpRequestStore()
        self.async_pylon_client_provider = async_pylon_client_provider or DEFAULT_ASYNC_PYLON_CLIENT_PROVIDER

    @override
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return AsyncHttpNeuronCommunicatorActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class AsyncHttpNeuronCommunicatorActor[InputModel: BaseModel, OutputModel: BaseModel](
    CommunicatorActor[InputModel, OutputModel]
):
    _SWEEP_TIMEOUT: datetime.timedelta = datetime.timedelta(milliseconds=50)
    _SENDER_QUEUE_ENQUEUE_TIMEOUT: datetime.timedelta = datetime.timedelta(milliseconds=50)
    _SENDER_THREAD_START_TIMEOUT: datetime.timedelta = datetime.timedelta(seconds=1)
    _SENDER_QUEUE_MAX_SIZE: int = 1_024
    _BACKGROUND_THREAD_JOIN_TIMEOUT_SECONDS: float = 1.0

    spec: AsyncHttpNeuronCommunicator[InputModel, OutputModel]

    # Background runtimes are started/stopped in on_start/on_stop to ensure they are
    # only active while the actor is running.
    _sender: SenderLoopRuntime[InputModel] | None
    _callback_server: CallbackHttpServerRuntime[OutputModel] | None
    _timeout_sweep: TimeoutSweepRuntime | None

    def __init__(
        self,
        *,
        spec: AsyncHttpNeuronCommunicator[InputModel, OutputModel],
        pipe_to_bus: PipeToBus,
        context_store: ContextStore,
    ) -> None:
        super().__init__(spec=spec, pipe_to_bus=pipe_to_bus, context_store=context_store)
        self.spec = spec
        self._sender = None
        self._callback_server = None
        self._timeout_sweep = None

    @override
    def on_start(self) -> None:
        super().on_start()
        try:
            self._ensure_background_threads_started()
        except Exception:
            # on_start failures happen before the event loop can receive StopActorEvent.
            # Roll back partially started resources to avoid leaking background threads.
            self._stop_background_threads()
            raise

    @override
    def on_stop(self) -> None:
        super().on_stop()
        self._stop_background_threads()

    def _ensure_background_threads_started(self) -> None:
        if self._sender is not None or self._callback_server is not None or self._timeout_sweep is not None:
            raise InternalFrameworkException(
                "Background runtimes are already initialized before startup. "
                "Expected sender/callback_server/timeout_sweep to be None."
            )

        processed_callback = CommunicatorProcessedCallback[OutputModel](
            emit_processed=lambda ctx_id, payload: self._emit(self._processed_event(ctx_id, payload)),
        )
        error_callback = CommunicatorErrorCallback(
            emit_executor_error=lambda ctx_id, error: self._emit(self._executor_error_event(ctx_id, error)),
        )

        self._sender = SenderLoopRuntime.start(
            config=SenderLoopRuntimeConfig(
                communicator_id=self.spec.id,
                queue_max_size=self._SENDER_QUEUE_MAX_SIZE,
                queue_enqueue_timeout=self._SENDER_QUEUE_ENQUEUE_TIMEOUT,
                send_timeout=self.spec.send_timeout,
                max_in_flight=self.spec.max_in_flight,
                total_processing_timeout=self.spec.total_processing_timeout,
                callback_base_url=self.spec.callback_base_url,
                response_path=self.spec.response_path,
                input_model=self.spec.input_model,
                pylon_client=self.spec.async_pylon_client_provider.get_client(),
            ),
            dependencies=SenderLoopRuntimeDependencies(
                pending_request_store=self.spec.pending_request_store,
                error_callback=error_callback,
            ),
            startup=SenderLoopRuntimeStartup(
                thread_name=f"AsyncHttpNeuronCommunicatorSender-{self.spec.id}",
                start_timeout=self._SENDER_THREAD_START_TIMEOUT,
                startup_failure_join_timeout_seconds=self._BACKGROUND_THREAD_JOIN_TIMEOUT_SECONDS,
            ),
        )
        self._callback_server = CallbackHttpServerRuntime.start(
            config=CallbackHttpServerRuntimeConfig(
                communicator_id=self.spec.id,
                bind_host=self.spec.callback_bind_ip,
                bind_port=self.spec.callback_port,
                response_path=self.spec.response_path,
                output_model=self.spec.output_model,
            ),
            dependencies=CallbackHttpServerRuntimeDependencies(
                pending_request_store=self.spec.pending_request_store,
                processed_callback=processed_callback,
                error_callback=error_callback,
            ),
            startup=CallbackHttpServerRuntimeStartup(
                thread_name=f"AsyncHttpNeuronCommunicatorHTTP-{self.spec.id}",
            ),
        )
        logger.info(
            "AsyncHttpNeuronCommunicator listening for responses on host=%s port=%s path=%r",
            self.spec.callback_bind_ip,
            self._callback_server.bound_port,
            self.spec.response_path,
        )
        self._timeout_sweep = TimeoutSweepRuntime.start(
            config=TimeoutSweepRuntimeConfig(
                communicator_id=self.spec.id,
                sweep_timeout=self._SWEEP_TIMEOUT,
            ),
            dependencies=TimeoutSweepRuntimeDependencies(
                pending_request_store=self.spec.pending_request_store,
                error_callback=error_callback,
            ),
            startup=TimeoutSweepRuntimeStartup(
                thread_name=f"AsyncHttpNeuronCommunicatorTimeout-{self.spec.id}",
            ),
        )

    def _stop_background_threads(self) -> None:
        sender = self._sender
        timeout_sweep = self._timeout_sweep
        callback_server = self._callback_server
        self._sender = None
        self._timeout_sweep = None
        self._callback_server = None

        if timeout_sweep is not None:
            try:
                timeout_sweep.stop(join_timeout_seconds=self._BACKGROUND_THREAD_JOIN_TIMEOUT_SECONDS)
            except Exception as exc:
                logger.error("Failed to stop timeout sweep runtime cleanly.", exc_info=exc)

        if sender is not None:
            try:
                sender.stop(
                    enqueue_timeout=self._SENDER_QUEUE_ENQUEUE_TIMEOUT,
                    join_timeout_seconds=self._BACKGROUND_THREAD_JOIN_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                logger.error("Failed to stop sender runtime cleanly.", exc_info=exc)

        if callback_server is not None:
            try:
                callback_server.stop()
            except Exception as exc:
                logger.error("Failed to stop callback server runtime cleanly.", exc_info=exc)

    @override
    def handle_input(self, ctx: Context, event: ReceiveEvent[Routed[InputModel]]) -> MessagesToSend:
        sender = self._sender
        if sender is None:
            self._emit_internal_error(event.ctx_id, InternalFrameworkException("Sender loop is not running."))
            return ()

        try:
            target_url = self._resolve_neuron_target_url(event.payload.target)
            sender.dispatch(
                ctx_id=event.ctx_id,
                target_url=target_url,
                target_neuron=event.payload.target,
                payload=event.payload.input,
            )
        except NexusException as exc:
            self._emit_internal_error(event.ctx_id, exc)
        except Exception as exc:
            self._emit_internal_error(
                event.ctx_id,
                RemoteRequestFailedException(
                    f"Unexpected failure while dispatching request for context {event.ctx_id}: {exc!r}"
                ),
            )
        return ()

    def _resolve_neuron_target_url(self, target: Neuron) -> AnyHttpUrl:
        axon_info = target.axon_info
        if axon_info.protocol != AxonProtocol.HTTP:
            raise UnsupportedAxonProtocolException(
                expected_protocol=AxonProtocol.HTTP,
                actual_protocol=axon_info.protocol,
            )

        if int(axon_info.port) <= 0 or int(axon_info.port) > 65535:
            raise NeuronAddressInvalidException(
                f"Target neuron {target.hotkey!r} has invalid HTTP port={int(axon_info.port)}."
            )

        target_host = format_host_for_url(axon_info.ip)
        target_url = f"http://{target_host}:{int(axon_info.port)}{self.spec.target_path}"
        return _ANY_HTTP_URL_ADAPTER.validate_python(target_url)
