from importlib import import_module
from typing import TYPE_CHECKING, Any

from nexus._internal.actors.chain_beat.block_beat import BlockBeatActor, BlockBeatNode
from nexus._internal.actors.chain_beat.epoch_beat import EpochBeatActor, EpochBeatNode
from nexus._internal.actors.executor_communicator import (
    AsyncHttpNeuronCommunicator,
    AsyncHttpNeuronCommunicatorActor,
    AsyncHttpNeuronService,
    ExecutorCommunicator,
    HttpBindEndpoint,
    InMemoryPendingAsyncHttpRequestStore,
    PendingAsyncHttpRequest,
    PendingAsyncHttpRequestStore,
)
from nexus._internal.actors.metagraph_source import MetagraphSource, NeuronMap, TriggeredMetagraph
from nexus._internal.actors.neuron_router import (
    Neuron,
    NeuronFilter,
    NeuronRouter,
    NeuronRouterActor,
    NoRoutableNeuronsException,
    RoundRobinNeuronRouter,
    Routed,
    keep_all_neurons,
    miners_only,
    validators_only,
)
from nexus._internal.actors.openrouter_client_provider import (
    OpenRouterClientProvider,
)
from nexus._internal.actors.openrouter_selection import (
    Fields,
    FieldValue,
    FileField,
    ImageUrlField,
    InputAudioField,
    ScalarField,
    VideoUrlField,
)
from nexus._internal.actors.pylon_client_provider import (
    PylonClientProvider,
)
from nexus._internal.actors.rest_entry_point import RestEntryPoint, RestEntryPointActor
from nexus._internal.actors.timestamper import Timestamped, TimestamperActor, TimestamperNode
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

if TYPE_CHECKING:
    from nexus._internal.actors.executor_communicator.openrouter_inference_communicator import (
        OpenRouterInferenceCommunicator,
        OpenRouterInferenceCommunicatorActor,
    )
    from nexus._internal.actors.openrouter_payload_creator import (
        MultiOpenRouterPayloadCreator,
        OpenRouterInferenceRequest,
    )
    from nexus._internal.actors.task_input_output_creator import (
        BatchedTaskInputOutput,
        TaskInputOutput,
        TaskInputOutputCreator,
    )
    from nexus._internal.actors.task_result_sampler import EveryTaskResultSampler

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "BatchedTaskInputOutput": ("nexus._internal.actors.task_input_output_creator", "BatchedTaskInputOutput"),
    "MultiOpenRouterPayloadCreator": (
        "nexus._internal.actors.openrouter_payload_creator",
        "MultiOpenRouterPayloadCreator",
    ),
    "OpenRouterInferenceRequest": ("nexus._internal.actors.openrouter_payload_creator", "OpenRouterInferenceRequest"),
    "EveryTaskResultSampler": ("nexus._internal.actors.task_result_sampler", "EveryTaskResultSampler"),
    "TaskInputOutput": ("nexus._internal.actors.task_input_output_creator", "TaskInputOutput"),
    "TaskInputOutputCreator": ("nexus._internal.actors.task_input_output_creator", "TaskInputOutputCreator"),
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
