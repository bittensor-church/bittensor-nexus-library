# pyright: basic

from contextlib import contextmanager

import pytest
from nexus_task_test_setup import (
    DummyTaskInput,
    build_nexus_task_test_setup,
)
from pydantic_settings import BaseSettings

from nexus.actors.payload_creator import NoopPayloadCreator
from nexus.core.dsl.nodes import Sink, Source
from nexus.nexus_validator import NexusValidator
from nexus.utils.exceptions import SubnetMisconfiguredException
from nexus.utils.subnet_settings import get_subnet_settings_as


class _TestSettings(BaseSettings):
    pass


def test_validator_construction_does_not_register_passed_settings_as_subnet_settings() -> None:
    settings = _TestSettings()

    NexusValidator(settings)

    with pytest.raises(SubnetMisconfiguredException):
        get_subnet_settings_as(_TestSettings)


def test_run_initializes_subnet_settings_before_validator_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_settings: list[_TestSettings | None] = []

    class _RunValidator(NexusValidator):
        def __init__(self, settings: BaseSettings) -> None:
            try:
                observed_settings.append(get_subnet_settings_as(_TestSettings))
            except SubnetMisconfiguredException:
                observed_settings.append(None)
            super().__init__(settings)

    @contextmanager
    def _fake_runtime(self: NexusValidator, shutdown_timeout_seconds: float = 30.0):
        yield object()

    def _stop_immediately(_seconds: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(_RunValidator, "start_runtime", _fake_runtime)
    monkeypatch.setattr("nexus.nexus_validator.time.sleep", _stop_immediately)

    _RunValidator.run(settings_class=_TestSettings)

    assert len(observed_settings) == 1
    assert isinstance(observed_settings[0], _TestSettings)


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
            self.connect(task.successful_task_result, Sink("task-downstream"))

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
