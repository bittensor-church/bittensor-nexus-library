from importlib import import_module
from typing import TYPE_CHECKING, Any

from nexus._internal.utils.exceptions import (
    AsyncHttpNeuronCommunicatorException,
    NeuronAddressInvalidException,
    RemoteExecutionException,
    RemoteRequestFailedException,
    RemoteRequestRejectedException,
    RemoteResponseTimeoutException,
    ResponseInvalidException,
    ResponseValidationException,
    UnsupportedAxonProtocolException,
)

from .async_http_neuron_communicator import (
    AsyncHttpNeuronCommunicator,
    AsyncHttpNeuronCommunicatorActor,
    HttpBindEndpoint,
)
from .async_http_neuron_service import AsyncHttpNeuronService
from .async_http_protocol import (
    AsyncHttpNeuronRequestEnvelope,
    AsyncHttpNeuronResponseEnvelope,
    RequestId,
)
from .base_communicator import CommunicatorActor, ExecutorCommunicator, ProcessedInput
from .common import (
    NormalizedHttpPath,
    UrlHost,
    format_host_for_url,
    normalize_http_path,
    timeout_seconds,
    validate_positive_timeout,
)
from .pending_requests import (
    InMemoryPendingAsyncHttpRequestStore,
    PendingAsyncHttpRequest,
    PendingAsyncHttpRequestStore,
)

if TYPE_CHECKING:
    from .openrouter_inference_communicator import (
        OpenRouterInferenceCommunicator,
        OpenRouterInferenceCommunicatorActor,
    )

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "OpenRouterInferenceCommunicator": (
        "nexus._internal.actors.executor_communicator.openrouter_inference_communicator",
        "OpenRouterInferenceCommunicator",
    ),
    "OpenRouterInferenceCommunicatorActor": (
        "nexus._internal.actors.executor_communicator.openrouter_inference_communicator",
        "OpenRouterInferenceCommunicatorActor",
    ),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY_EXPORTS:
        module_name, attr_name = _LAZY_EXPORTS[name]
        value = getattr(import_module(module_name), attr_name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AsyncHttpNeuronCommunicator",
    "AsyncHttpNeuronCommunicatorActor",
    "AsyncHttpNeuronCommunicatorException",
    "AsyncHttpNeuronRequestEnvelope",
    "AsyncHttpNeuronResponseEnvelope",
    "AsyncHttpNeuronService",
    "CommunicatorActor",
    "ExecutorCommunicator",
    "ProcessedInput",
    "NormalizedHttpPath",
    "OpenRouterInferenceCommunicator",
    "OpenRouterInferenceCommunicatorActor",
    "UrlHost",
    "format_host_for_url",
    "HttpBindEndpoint",
    "InMemoryPendingAsyncHttpRequestStore",
    "NeuronAddressInvalidException",
    "RemoteRequestFailedException",
    "RemoteRequestRejectedException",
    "ResponseInvalidException",
    "RemoteResponseTimeoutException",
    "ResponseValidationException",
    "normalize_http_path",
    "PendingAsyncHttpRequest",
    "PendingAsyncHttpRequestStore",
    "RequestId",
    "RemoteExecutionException",
    "UnsupportedAxonProtocolException",
    "timeout_seconds",
    "validate_positive_timeout",
]
