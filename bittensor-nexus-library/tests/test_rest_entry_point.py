# pyright: basic

from collections.abc import Callable
from typing import Any, override

import requests
from pydantic import BaseModel
from pylon_client.artanis import Port
from utils import wait_until

from nexus.actors.payload_creator import NoopPayloadCreator
from nexus.actors.rest_entry_point import RestEntryPoint, RestEntryPointActor
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Node, Transform
from nexus.core.runtime.actor import Actor, ActorBuilder
from nexus.core.runtime.actor_patterns import TransformActor
from nexus.core.runtime.context_store import Context, ContextStore
from nexus.core.runtime.events import PipeToBus
from nexus.core.runtime.subnet_runtime import SubnetBuilder, SubnetRuntime

HTTP_TIMEOUT_SECONDS = 3.0
STARTUP_TIMEOUT_SECONDS = 3.0
SHUTDOWN_TIMEOUT_SECONDS = 5.0


class UserRequest(BaseModel):
    text: str


class ErroringResponder(Transform[UserRequest, UserRequest], ActorBuilder):
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return ErroringResponderActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class ErroringResponderActor(TransformActor[UserRequest, UserRequest]):
    @override
    def _transform(self, ctx: Context, payload: UserRequest) -> UserRequest:
        del ctx, payload
        raise ValueError("boom")


class NotSerializable:
    value: str

    def __init__(self, value: str) -> None:
        self.value = value


class NonSerializableResponder(Transform[UserRequest, object], ActorBuilder):
    def build_actor(self, *, pipe_to_bus: PipeToBus, context_store: ContextStore) -> Actor:
        return NonSerializableResponderActor(spec=self, pipe_to_bus=pipe_to_bus, context_store=context_store)


class NonSerializableResponderActor(TransformActor[UserRequest, object]):
    @override
    def _transform(self, ctx: Context, payload: UserRequest) -> object:
        del ctx
        return NotSerializable(payload.text)


def _build_runtime(*, nodes: tuple[Node, ...], flows: tuple[Flow, ...]) -> SubnetRuntime:
    builder = SubnetBuilder(nodes=nodes, include_node_flows=False)
    builder.add_flows(*flows)
    return builder.build()


def _entry_url(entry: RestEntryPoint[UserRequest]) -> str:
    return f"http://127.0.0.1:{int(entry.port)}{entry.path}"


def _wait_until_server_ready(url: str) -> None:
    def _ready() -> bool:
        try:
            requests.get(url, timeout=0.25)
            return True
        except requests.RequestException:
            return False

    wait_until(_ready, timeout=STARTUP_TIMEOUT_SECONDS, interval=0.05)


def test_rest_entry_point_returns_json_payload_on_success(
    unused_local_port: Callable[[], Port],
) -> None:
    port = unused_local_port()
    entry = RestEntryPoint(
        _id="rest-entry-point-success",
        bind_ip="127.0.0.1",
        path="/rest",
        port=port,
        user_data_model=UserRequest,
    )
    passthrough = NoopPayloadCreator[UserRequest]("rest-entry-point-passthrough")

    runtime = _build_runtime(
        nodes=(entry, passthrough),
        flows=(
            Flow.from_connectable(entry.source).then(passthrough.input),
            Flow.from_connectable(passthrough.created_payload).then(entry.sink),
        ),
    )
    url = _entry_url(entry)

    with runtime.running(shutdown_timeout_seconds=SHUTDOWN_TIMEOUT_SECONDS):
        _wait_until_server_ready(url)
        response = requests.post(url, json={"text": "hello"}, timeout=HTTP_TIMEOUT_SECONDS)

    assert response.status_code == 200
    assert response.headers["Content-Type"].startswith("application/json")
    assert response.json() == {"text": "hello"}


def test_rest_entry_point_returns_json_error_on_validation_failure(
    unused_local_port: Callable[[], Port],
) -> None:
    port = unused_local_port()
    entry = RestEntryPoint(
        _id="rest-entry-point-validation",
        bind_ip="127.0.0.1",
        path="/rest",
        port=port,
        user_data_model=UserRequest,
    )
    passthrough = NoopPayloadCreator[UserRequest]("rest-entry-point-validation-passthrough")

    runtime = _build_runtime(
        nodes=(entry, passthrough),
        flows=(
            Flow.from_connectable(entry.source).then(passthrough.input),
            Flow.from_connectable(passthrough.created_payload).then(entry.sink),
        ),
    )
    url = _entry_url(entry)

    with runtime.running(shutdown_timeout_seconds=SHUTDOWN_TIMEOUT_SECONDS):
        _wait_until_server_ready(url)
        response = requests.post(url, json={"missing": "field"}, timeout=HTTP_TIMEOUT_SECONDS)

    error = response.json()["error"]
    assert response.status_code == 400
    assert response.headers["Content-Type"].startswith("application/json")
    assert error["type"] == "ValidationError"
    assert "Invalid request body" in error["message"]


def test_rest_entry_point_returns_json_errors_for_404_and_405(
    unused_local_port: Callable[[], Port],
) -> None:
    port = unused_local_port()
    entry = RestEntryPoint(
        _id="rest-entry-point-routing-errors",
        bind_ip="127.0.0.1",
        path="/rest",
        port=port,
        user_data_model=UserRequest,
    )
    passthrough = NoopPayloadCreator[UserRequest]("rest-entry-point-routing-errors-passthrough")

    runtime = _build_runtime(
        nodes=(entry, passthrough),
        flows=(
            Flow.from_connectable(entry.source).then(passthrough.input),
            Flow.from_connectable(passthrough.created_payload).then(entry.sink),
        ),
    )
    url = _entry_url(entry)

    with runtime.running(shutdown_timeout_seconds=SHUTDOWN_TIMEOUT_SECONDS):
        _wait_until_server_ready(url)
        method_not_allowed = requests.get(url, timeout=HTTP_TIMEOUT_SECONDS)
        not_found = requests.post(f"http://127.0.0.1:{int(entry.port)}/other", timeout=HTTP_TIMEOUT_SECONDS)

    assert method_not_allowed.status_code == 405
    assert method_not_allowed.headers["Content-Type"].startswith("application/json")
    assert method_not_allowed.json()["error"]["type"] == "MethodNotAllowed"

    assert not_found.status_code == 404
    assert not_found.headers["Content-Type"].startswith("application/json")
    assert not_found.json()["error"]["type"] == "NotFound"


def test_rest_entry_point_returns_500_for_task_exception(
    unused_local_port: Callable[[], Port],
) -> None:
    port = unused_local_port()
    entry = RestEntryPoint(
        _id="rest-entry-point-task-exception",
        bind_ip="127.0.0.1",
        path="/rest",
        port=port,
        user_data_model=UserRequest,
    )
    failing = ErroringResponder("rest-entry-point-erroring-responder")

    runtime = _build_runtime(
        nodes=(entry, failing),
        flows=(
            Flow.from_connectable(entry.source).then(failing.sink),
            Flow.from_connectable(failing.error).then(entry.sink),
        ),
    )
    url = _entry_url(entry)

    with runtime.running(shutdown_timeout_seconds=SHUTDOWN_TIMEOUT_SECONDS):
        _wait_until_server_ready(url)
        response = requests.post(url, json={"text": "hello"}, timeout=HTTP_TIMEOUT_SECONDS)

    error = response.json()["error"]
    causes = error["causes"]
    assert response.status_code == 500
    assert response.headers["Content-Type"].startswith("application/json")
    assert error["type"] == "SafeInvokeWrappedException"
    assert isinstance(causes, list)
    assert len(causes) >= 1
    assert causes[0]["type"] == "ValueError"
    assert causes[0]["message"] == "boom"


def test_rest_entry_point_returns_500_when_output_is_not_json_serializable(
    unused_local_port: Callable[[], Port],
) -> None:
    port = unused_local_port()
    entry = RestEntryPoint(
        _id="rest-entry-point-serialization-error",
        bind_ip="127.0.0.1",
        path="/rest",
        port=port,
        user_data_model=UserRequest,
    )
    non_serializable = NonSerializableResponder("rest-entry-point-non-serializable")

    runtime = _build_runtime(
        nodes=(entry, non_serializable),
        flows=(
            Flow.from_connectable(entry.source).then(non_serializable.sink),
            Flow.from_connectable(non_serializable.ok).then(entry.sink),
        ),
    )
    url = _entry_url(entry)

    with runtime.running(shutdown_timeout_seconds=SHUTDOWN_TIMEOUT_SECONDS):
        _wait_until_server_ready(url)
        response = requests.post(url, json={"text": "hello"}, timeout=HTTP_TIMEOUT_SECONDS)

    error = response.json()["error"]
    assert response.status_code == 500
    assert error["type"] == "ResponseSerializationError"

    causes = error["causes"]
    assert isinstance(causes, list)
    assert len(causes) >= 1
    assert causes[0]["type"] == "PydanticSerializationError"


def test_rest_entry_point_returns_504_when_downstream_does_not_respond(
    unused_local_port: Callable[[], Port],
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(RestEntryPointActor, "_RESPONSE_TIMEOUT_S", 0.2)

    port = unused_local_port()
    entry = RestEntryPoint(
        _id="rest-entry-point-timeout",
        bind_ip="127.0.0.1",
        path="/rest",
        port=port,
        user_data_model=UserRequest,
    )
    consume_only = NoopPayloadCreator[UserRequest]("rest-entry-point-timeout-consume-only")

    runtime = _build_runtime(
        nodes=(entry, consume_only),
        flows=(Flow.from_connectable(entry.source).then(consume_only.input),),
    )
    url = _entry_url(entry)

    with runtime.running(shutdown_timeout_seconds=SHUTDOWN_TIMEOUT_SECONDS):
        _wait_until_server_ready(url)
        response = requests.post(url, json={"text": "hello"}, timeout=HTTP_TIMEOUT_SECONDS)

    assert response.status_code == 504
    assert response.headers["Content-Type"].startswith("application/json")
    assert response.json()["error"]["type"] == "GatewayTimeout"
