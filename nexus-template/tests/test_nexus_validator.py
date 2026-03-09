# pyright: basic

from nexus_task_test_setup import (
    DummyExecutorOutput,
    DummyTaskInput,
    build_nexus_task_test_setup,
)
from pydantic_settings import BaseSettings

from nexus.actors.payload_creator import NoopPayloadCreator
from nexus.core.dsl.nodes import Sink, Source
from nexus.nexus_validator import NexusValidator
from nexus.utils.exceptions import NexusException


class _TestSettings(BaseSettings):
    pass


def test_connect_discovers_node_without_validator_field() -> None:
    class _LocalNodeValidator(NexusValidator):
        def __init__(self) -> None:
            super().__init__(_TestSettings())
            local_node = NoopPayloadCreator[str]("validator-local-node")
            self.connect(Source[str]("external-upstream"), local_node.input)
            self.connect(local_node.created_payload, Sink[str]("external-downstream"))

    runtime = _LocalNodeValidator()._build_runtime()

    assert len(runtime.actors) == 1
    assert any("validator-local-node" in actor.actor_id for actor in runtime.actors)


def test_build_runtime_includes_only_connected_nodes() -> None:
    class _OnlyConnectedValidator(NexusValidator):
        def __init__(self) -> None:
            super().__init__(_TestSettings())
            connected = NoopPayloadCreator[str]("validator-connected-node")
            NoopPayloadCreator[str]("validator-unused-node")
            self.connect(Source[str]("external-upstream"), connected.input)
            self.connect(connected.created_payload, Sink[str]("external-downstream"))

    runtime = _OnlyConnectedValidator()._build_runtime()

    assert len(runtime.actors) == 1
    assert any("validator-connected-node" in actor.actor_id for actor in runtime.actors)
    assert not any("validator-unused-node" in actor.actor_id for actor in runtime.actors)


def test_connect_discovers_task_from_task_endpoints() -> None:
    task_setup = build_nexus_task_test_setup()
    task = task_setup.task

    class _TaskValidator(NexusValidator):
        def __init__(self) -> None:
            super().__init__(_TestSettings())
            self.connect(Source[DummyTaskInput]("task-upstream"), task.input)
            self.connect(task.executor_output, Sink[DummyExecutorOutput | NexusException]("task-downstream"))

    runtime = _TaskValidator()._build_runtime()

    expected_actor_count = len(task.internal_nodes()) + 1  # + internal subnet clock
    assert len(runtime.actors) == expected_actor_count
    assert any("internal-subnet-clock" in actor.actor_id for actor in runtime.actors)


def test_build_runtime_allows_ownerless_external_endpoints() -> None:
    class _OwnerlessEndpointsValidator(NexusValidator):
        def __init__(self) -> None:
            super().__init__(_TestSettings())
            self.connect(Source[str]("ownerless-upstream"), Sink[str]("ownerless-downstream"))

    runtime = _OwnerlessEndpointsValidator()._build_runtime()

    assert runtime.actors == ()
