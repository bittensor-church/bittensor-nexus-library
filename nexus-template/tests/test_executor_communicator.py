# pyright: basic

import datetime
from collections.abc import Callable
from ipaddress import IPv4Address

import pytest
from pydantic import BaseModel
from pylon_client.artanis import Port
from pylon_client.artanis.v1 import AxonInfo, AxonProtocol, Neuron
from transform_test_utils import TransformActorTestSetupFactory
from utils import build_neuron, wait_until

from nexus.actors.executor_communicator import (
    AsyncHttpNeuronCommunicator,
    AsyncHttpNeuronService,
    HttpBindEndpoint,
    ProcessedInput,
    RemoteExecutionException,
    RemoteRequestFailedException,
    RemoteResponseTimeoutException,
    UnsupportedAxonProtocolException,
)
from nexus.actors.executor_communicator.embedded_executor_communicator import EmbeddedExecutorCommunicator
from nexus.actors.neuron_router import Routed
from nexus.utils.exceptions import EmbeddedExecutorFailureException, ExecutorFailureException

DEFAULT_SERVICE_PATH = "/process"
DEFAULT_RESPONSE_PATH = "/response"
DEFAULT_CALLBACK_TIMEOUT = datetime.timedelta(seconds=5)
DEFAULT_SEND_TIMEOUT = datetime.timedelta(seconds=1.0)
DEFAULT_TOTAL_PROCESSING_TIMEOUT = datetime.timedelta(seconds=2.0)


class CommunicatorInput(BaseModel):
    text: str


class CommunicatorOutput(BaseModel):
    text: str


type Communicator = AsyncHttpNeuronCommunicator[CommunicatorInput, CommunicatorOutput]
type Service = AsyncHttpNeuronService[CommunicatorInput, CommunicatorOutput]
type Processor = Callable[[CommunicatorInput], CommunicatorOutput]
type ResponseListener = tuple[HttpBindEndpoint, str]
type ServiceFactory = Callable[..., Service]
type CommunicatorFactory = Callable[..., Communicator]


def build_test_neuron(*, port: Port, protocol: AxonProtocol = AxonProtocol.HTTP) -> Neuron:
    neuron = build_neuron(uid=1, hotkey=f"neuron-{port}", validator_permit=False)
    return neuron.model_copy(
        update={
            "axon_info": AxonInfo(ip=IPv4Address("127.0.0.1"), port=Port(port), protocol=protocol),
        }
    )


def _local_response_listener(
    *,
    next_unused_local_port: Callable[[], Port],
) -> tuple[HttpBindEndpoint, str]:
    port = next_unused_local_port()
    return HttpBindEndpoint(host=IPv4Address("127.0.0.1"), port=port), f"http://127.0.0.1:{int(port)}"


@pytest.fixture
def response_listener(unused_local_port: Callable[[], Port]) -> ResponseListener:
    return _local_response_listener(next_unused_local_port=unused_local_port)


@pytest.fixture
def async_http_neuron_service_factory() -> ServiceFactory:
    def _build(
        *,
        processor: Processor | None = None,
        callback_timeout: datetime.timedelta = DEFAULT_CALLBACK_TIMEOUT,
        path: str = DEFAULT_SERVICE_PATH,
        port: Port | None = None,
    ) -> Service:
        return AsyncHttpNeuronService[CommunicatorInput, CommunicatorOutput](
            path=path,
            port=port or Port(0),
            input_model=CommunicatorInput,
            output_model=CommunicatorOutput,
            processor=processor or (lambda payload: CommunicatorOutput(text=payload.text.upper())),
            callback_timeout=callback_timeout,
        )

    return _build


@pytest.fixture
def communicator_factory(
    response_listener: ResponseListener,
) -> CommunicatorFactory:
    response_bind, callback_base_url = response_listener

    def _build(
        *,
        communicator_id: str,
        send_timeout: datetime.timedelta = DEFAULT_SEND_TIMEOUT,
        total_processing_timeout: datetime.timedelta = DEFAULT_TOTAL_PROCESSING_TIMEOUT,
        callback_url_override: str | None = None,
        target_path: str = DEFAULT_SERVICE_PATH,
        response_path: str = DEFAULT_RESPONSE_PATH,
    ) -> Communicator:
        return AsyncHttpNeuronCommunicator[CommunicatorInput, CommunicatorOutput](
            communicator_id,
            target_path=target_path,
            send_timeout=send_timeout,
            total_processing_timeout=total_processing_timeout,
            callback_bind_ip=response_bind,
            callback_path=response_path,
            callback_base_url=callback_url_override or callback_base_url,
            input_model=CommunicatorInput,
            output_model=CommunicatorOutput,
        )

    return _build


def test_async_http_neuron_communicator_emits_processed_on_valid_response(
    async_http_neuron_service_factory: ServiceFactory,
    communicator_factory: CommunicatorFactory,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    service = async_http_neuron_service_factory()

    with service.running():
        communicator = communicator_factory(communicator_id="communicator-happy-path")
        setup = transform_actor_test_setup_factory(communicator)
        neuron = build_test_neuron(port=service.bound_port)

        with setup.running():
            routed_input = Routed(input=CommunicatorInput(text="hello"), target=neuron)
            setup.send(
                input_payload=routed_input,
            )
            wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=3.0)

        processed_payload = setup.processed_collector.received_events[0].payload
        assert processed_payload == ProcessedInput(
            input=routed_input,
            output=CommunicatorOutput(text="HELLO"),
        )
        assert len(setup.error_collector.received_events) == 0


def test_async_http_neuron_communicator_emits_error_when_target_request_fails(
    communicator_factory: CommunicatorFactory,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
    unused_local_port: Callable[[], Port],
) -> None:
    unavailable_port = unused_local_port()
    communicator = communicator_factory(
        communicator_id="communicator-send-failure",
        send_timeout=datetime.timedelta(seconds=0.2),
        total_processing_timeout=datetime.timedelta(seconds=1.0),
    )
    setup = transform_actor_test_setup_factory(communicator)
    neuron = build_test_neuron(port=unavailable_port)
    routed_input = Routed(input=CommunicatorInput(text="hello"), target=neuron)

    with setup.running():
        setup.send(input_payload=routed_input)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

    assert len(setup.error_collector.received_events) == 0
    failure = setup.processed_collector.received_events[0].payload
    assert isinstance(failure.output, ExecutorFailureException)
    assert failure.input == routed_input
    assert isinstance(failure.output.executor_error, RemoteRequestFailedException)


def test_async_http_neuron_communicator_emits_timeout_when_no_callback_is_received(
    async_http_neuron_service_factory: ServiceFactory,
    communicator_factory: CommunicatorFactory,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    service = async_http_neuron_service_factory(
        callback_timeout=datetime.timedelta(seconds=0.1),
    )

    with service.running():
        communicator = communicator_factory(
            communicator_id="communicator-timeout",
            total_processing_timeout=datetime.timedelta(seconds=0.3),
            callback_url_override="http://invalid-url-so-service-doesnt-connect:65530",
        )
        setup = transform_actor_test_setup_factory(communicator)
        neuron = build_test_neuron(port=service.bound_port)
        routed_input = Routed(input=CommunicatorInput(text="hello"), target=neuron)

        with setup.running():
            setup.send(input_payload=routed_input)
            wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

        assert len(setup.error_collector.received_events) == 0
        failure = setup.processed_collector.received_events[0].payload
        assert isinstance(failure.output, ExecutorFailureException)
        assert failure.input == routed_input
        assert isinstance(failure.output.executor_error, RemoteResponseTimeoutException)


def test_async_http_neuron_communicator_emits_remote_error_when_service_processing_fails(
    async_http_neuron_service_factory: ServiceFactory,
    communicator_factory: CommunicatorFactory,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    service = async_http_neuron_service_factory(
        processor=lambda _: (_ for _ in ()).throw(RuntimeError("service boom")),
    )

    with service.running():
        communicator = communicator_factory(communicator_id="communicator-remote-error")
        setup = transform_actor_test_setup_factory(communicator)
        neuron = build_test_neuron(port=service.bound_port)
        routed_input = Routed(input=CommunicatorInput(text="hello"), target=neuron)

        with setup.running():
            setup.send(input_payload=routed_input)
            wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

        assert len(setup.error_collector.received_events) == 0
        failure = setup.processed_collector.received_events[0].payload
        assert isinstance(failure.output, ExecutorFailureException)
        assert failure.input == routed_input
        assert isinstance(failure.output.executor_error, RemoteExecutionException)


def test_async_http_neuron_communicator_rejects_non_http_axon_protocol(
    communicator_factory: CommunicatorFactory,
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    communicator = communicator_factory(communicator_id="communicator-protocol-check")
    setup = transform_actor_test_setup_factory(communicator)
    neuron = build_test_neuron(port=Port(1234), protocol=AxonProtocol.TCP)
    routed_input = Routed(input=CommunicatorInput(text="hello"), target=neuron)

    with setup.running():
        setup.send(input_payload=routed_input)
        wait_until(lambda: len(setup.error_collector.received_events) == 1, timeout=2.0)

    assert len(setup.processed_collector.received_events) == 0
    failure = setup.error_collector.received_events[0].payload
    assert isinstance(failure, UnsupportedAxonProtocolException)
    assert failure.expected_protocol == AxonProtocol.HTTP
    assert failure.actual_protocol == AxonProtocol.TCP


def test_embedded_executor_communicator_actor_emits_processed_on_happy_path(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    communicator = EmbeddedExecutorCommunicator[CommunicatorInput, CommunicatorOutput](
        "embedded-communicator-happy-path",
        input_model=CommunicatorInput,
        output_model=CommunicatorOutput,
        executor_func=lambda payload: CommunicatorOutput(text=payload.text.upper()),
    )
    setup = transform_actor_test_setup_factory(communicator)
    routed_input = Routed(input=CommunicatorInput(text="hello"), target=build_test_neuron(port=Port(1234)))

    with setup.running():
        setup.send(input_payload=routed_input)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

    assert len(setup.error_collector.received_events) == 0
    processed_payload = setup.processed_collector.received_events[0].payload
    assert processed_payload == ProcessedInput(
        input=routed_input,
        output=CommunicatorOutput(text="HELLO"),
    )


def test_embedded_executor_communicator_actor_emits_executor_failure_on_execution_error(
    transform_actor_test_setup_factory: TransformActorTestSetupFactory,
) -> None:
    def fail_executor(_: CommunicatorInput) -> CommunicatorOutput:
        raise RuntimeError("executor boom")

    communicator = EmbeddedExecutorCommunicator[CommunicatorInput, CommunicatorOutput](
        "embedded-communicator-failure-path",
        input_model=CommunicatorInput,
        output_model=CommunicatorOutput,
        executor_func=fail_executor,
    )
    setup = transform_actor_test_setup_factory(communicator)
    routed_input = Routed(input=CommunicatorInput(text="hello"), target=build_test_neuron(port=Port(1234)))

    with setup.running():
        setup.send(input_payload=routed_input)
        wait_until(lambda: len(setup.processed_collector.received_events) == 1, timeout=2.0)

    assert len(setup.error_collector.received_events) == 0
    failure = setup.processed_collector.received_events[0].payload
    assert failure.input == routed_input
    assert isinstance(failure.output, ExecutorFailureException)
    assert isinstance(failure.output.executor_error, EmbeddedExecutorFailureException)
    assert isinstance(failure.output.executor_error.__cause__, RuntimeError)
