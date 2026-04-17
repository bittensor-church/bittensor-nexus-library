from importlib import import_module
from typing import TYPE_CHECKING, Any

from nexus.actors.chain_beat.block_beat import BlockBeatNode, BlockBeatActor
from nexus.actors.chain_beat.epoch_beat import EpochBeatNode, EpochBeatActor
from nexus.actors.timestamper import TimestamperNode, TimestamperActor, Timestamped
from nexus.actors.pylon_client_provider import (
    PylonClientProvider,
)
from nexus.actors.openrouter_client_provider import (
    OpenRouterClientProvider,
)
from nexus.actors.executor_communicator import (
    AsyncHttpNeuronCommunicator,
    AsyncHttpNeuronCommunicatorActor,
    AsyncHttpNeuronService,
    ExecutorCommunicator,
    HttpBindEndpoint,
    InMemoryPendingAsyncHttpRequestStore,
    PendingAsyncHttpRequest,
    PendingAsyncHttpRequestStore,
)
from nexus.actors.metagraph_source import MetagraphSource, NeuronMap, TriggeredMetagraph
from nexus.actors.openrouter_selection import (
    FileField,
    ImageUrlField,
    InputAudioField,
    ScalarField,
    FieldValue,
    Fields,
    VideoUrlField,
)
from nexus.actors.rest_entry_point import RestEntryPoint, RestEntryPointActor
from nexus.actors.neuron_router import (
    NoRoutableNeuronsException,
    NeuronFilter,
    RoundRobinNeuronRouter,
    Routed,
    NeuronRouter,
    NeuronRouterActor,
    Neuron,
    keep_all_neurons,
    miners_only,
    validators_only,
)
from nexus.utils.exceptions import (
    AsyncHttpNeuronCommunicatorException,
    NeuronAddressInvalidException,
    RemoteRequestFailedException,
    RemoteRequestRejectedException,
    ResponseInvalidException,
    RemoteResponseTimeoutException,
    ResponseValidationException,
    RemoteExecutionException,
    UnsupportedAxonProtocolException,
)

if TYPE_CHECKING:
    from nexus.actors.executor_communicator.openrouter_inference_communicator import (
        OpenRouterInferenceCommunicator,
        OpenRouterInferenceCommunicatorActor,
    )
    from nexus.actors.openrouter_payload_creator import MultiOpenRouterPayloadCreator, OpenRouterInferenceRequest
    from nexus.actors.task_input_output_creator import BatchedTaskInputOutput, TaskInputOutput, TaskInputOutputCreator
    from nexus.actors.task_result_sampler import EveryTaskResultSampler

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BatchedTaskInputOutput": ("nexus.actors.task_input_output_creator", "BatchedTaskInputOutput"),
    "MultiOpenRouterPayloadCreator": ("nexus.actors.openrouter_payload_creator", "MultiOpenRouterPayloadCreator"),
    "OpenRouterInferenceRequest": ("nexus.actors.openrouter_payload_creator", "OpenRouterInferenceRequest"),
    "EveryTaskResultSampler": ("nexus.actors.task_result_sampler", "EveryTaskResultSampler"),
    "TaskInputOutput": ("nexus.actors.task_input_output_creator", "TaskInputOutput"),
    "TaskInputOutputCreator": ("nexus.actors.task_input_output_creator", "TaskInputOutputCreator"),
    "OpenRouterInferenceCommunicator": (
        "nexus.actors.executor_communicator.openrouter_inference_communicator",
        "OpenRouterInferenceCommunicator",
    ),
    "OpenRouterInferenceCommunicatorActor": (
        "nexus.actors.executor_communicator.openrouter_inference_communicator",
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
    "BlockBeatNode",
    "BlockBeatActor",
    "EpochBeatNode",
    "EpochBeatActor",
    "TimestamperNode",
    "TimestamperActor",
    "Timestamped",
    "AsyncHttpNeuronCommunicator",
    "AsyncHttpNeuronCommunicatorActor",
    "AsyncHttpNeuronCommunicatorException",
    "AsyncHttpNeuronService",
    "BatchedTaskInputOutput",
    "ExecutorCommunicator",
    "FileField",
    "FieldValue",
    "Fields",
    "HttpBindEndpoint",
    "ImageUrlField",
    "InMemoryPendingAsyncHttpRequestStore",
    "InputAudioField",
    "MetagraphSource",
    "MultiOpenRouterPayloadCreator",
    "NeuronMap",
    "NeuronAddressInvalidException",
    "NeuronFilter",
    "OpenRouterInferenceCommunicator",
    "OpenRouterInferenceCommunicatorActor",
    "OpenRouterInferenceRequest",
    "RemoteRequestFailedException",
    "RemoteRequestRejectedException",
    "ResponseInvalidException",
    "RemoteResponseTimeoutException",
    "ResponseValidationException",
    "EveryTaskResultSampler",
    "NoRoutableNeuronsException",
    "OpenRouterClientProvider",
    "PendingAsyncHttpRequest",
    "PendingAsyncHttpRequestStore",
    "PylonClientProvider",
    "RemoteExecutionException",
    "RoundRobinNeuronRouter",
    "ScalarField",
    "RestEntryPoint",
    "RestEntryPointActor",
    "Routed",
    "TaskInputOutput",
    "TaskInputOutputCreator",
    "NeuronRouter",
    "NeuronRouterActor",
    "Neuron",
    "TriggeredMetagraph",
    "UnsupportedAxonProtocolException",
    "VideoUrlField",
    "keep_all_neurons",
    "miners_only",
    "validators_only",
]
