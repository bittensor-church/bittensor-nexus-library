# pyright: basic

from unittest.mock import create_autospec

from pydantic import BaseModel
from transform_test_utils import TransformActorTestSetupFactory
from utils import build_neuron, wait_until

from nexus.actors.executor_communicator import ProcessedInput
from nexus.actors.executor_communicator.openrouter_inference_communicator import (
    OpenRouterInferenceCommunicator,
)
from nexus.actors.neuron_router import Routed
from nexus.actors.openrouter_client_provider import OpenRouterClientProvider
from nexus.actors.openrouter_payload_creator import OpenRouterInferenceRequest
from nexus.utils.exceptions import ExecutorFailureException, NexusException, SubnetMisconfiguredException
from nexus.utils.openrouter_client import OpenRouterClient


class ResponseModel(BaseModel):
    score: int


class _StaticOpenRouterClientProvider(OpenRouterClientProvider):
    def __init__(self, client: OpenRouterClient) -> None:
        self._client = client

    def get_client(self) -> OpenRouterClient:
        return self._client


def test_openrouter_inference_communicator_emits_processed_on_success(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    openrouter_client = create_autospec(OpenRouterClient, instance=True)
    openrouter_client.query.return_value = ResponseModel(score=88)
    communicator = OpenRouterInferenceCommunicator[ResponseModel](
        "openrouter-communicator",
        output_model=ResponseModel,
        openrouter_client_provider=_StaticOpenRouterClientProvider(openrouter_client),
    )
    setup = transform_actor_test_setup_factory(communicator)
    routed_input = Routed(
        input=OpenRouterInferenceRequest(
            fields=(),
            messages=({"role": "user", "content": [{"type": "text", "text": "hello"}]},),
        ),
        target=build_neuron(uid=1, hotkey="validator", validator_permit=True),
    )

    with setup.running():
        setup.send(input_payload=routed_input)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

    assert len(setup.error_collector.received_events) == 0
    openrouter_client.query.assert_called_once_with(
        messages=[{"role": "user", "content": [{"type": "text", "text": "hello"}]}],
        response_model=ResponseModel,
    )
    processed = setup.processed_collector.received_events[0].payload
    assert processed == ProcessedInput(
        input=routed_input,
        output=ResponseModel(score=88),
    )


def test_openrouter_inference_communicator_wraps_query_errors_as_executor_failures(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    openrouter_client = create_autospec(OpenRouterClient, instance=True)
    openrouter_client.query.side_effect = RuntimeError("boom")
    communicator = OpenRouterInferenceCommunicator[ResponseModel](
        "openrouter-communicator-error",
        output_model=ResponseModel,
        openrouter_client_provider=_StaticOpenRouterClientProvider(openrouter_client),
    )
    setup = transform_actor_test_setup_factory(communicator)
    routed_input = Routed(
        input=OpenRouterInferenceRequest(fields=(), messages=()),
        target=build_neuron(uid=1, hotkey="validator", validator_permit=True),
    )

    with setup.running():
        setup.send(input_payload=routed_input)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

    assert len(setup.error_collector.received_events) == 0
    processed = setup.processed_collector.received_events[0].payload
    assert processed.input == routed_input
    assert isinstance(processed.output, ExecutorFailureException)
    assert isinstance(processed.output.executor_error, NexusException)
    assert isinstance(processed.output.executor_error.__cause__, RuntimeError)


def test_openrouter_inference_communicator_emits_error_when_required_settings_missing(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    communicator = OpenRouterInferenceCommunicator[ResponseModel](
        "openrouter-communicator-config-error",
        output_model=ResponseModel,
    )
    setup = transform_actor_test_setup_factory(communicator)
    routed_input = Routed(
        input=OpenRouterInferenceRequest(fields=(), messages=()),
        target=build_neuron(uid=1, hotkey="validator", validator_permit=True),
    )

    with setup.running():
        setup.send(input_payload=routed_input)
        wait_until(lambda: len(setup.error_collector.received_events) == 1, timeout=2.0)

    assert len(setup.processed_collector.received_events) == 0
    error = setup.error_collector.received_events[0].payload
    assert isinstance(error, SubnetMisconfiguredException)
