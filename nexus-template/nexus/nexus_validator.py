# pyright: basic
import logging
import sys
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from pydantic import ValidationError
from pydantic_settings import BaseSettings

from nexus.actors import BlockBeatNode
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Node, NodeSinks, NodeSources, Sink, Source, SourceName
from nexus.core.runtime.nexus_task import NexusTask
from nexus.core.runtime.subnet_runtime import SubnetBuilder, SubnetRuntime

log = logging.getLogger("validator")


class NexusValidator:
    """Base class for validator graphs built from explicit `connect(...)` wiring.

    Runtime components are discovered only from endpoints used in `connect(...)`.
    There is no separate node/task registration API.
    """

    subnet_clock: BlockBeatNode

    _connected_nodes: dict[int, Node]
    _connected_tasks: dict[int, NexusTask[Any, Any, Any, Any]]
    subnet_flow: Flow
    runtime: SubnetRuntime | None = None

    def __init__(self, settings: BaseSettings) -> None:
        self.subnet_clock = BlockBeatNode("internal-subnet-clock")
        self._connected_nodes = {}
        self._connected_tasks = {}

        self.subnet_flow = Flow(
            entry_sinks=NodeSinks(sinks={}),
            exit_sources=NodeSources(sources={SourceName("internal-subnet-clock"): self.subnet_clock.source}),
        )

    @classmethod
    def run[SettingsModel: BaseSettings](
        cls,
        *,
        settings_class: type[SettingsModel],
    ) -> None:
        shutdown_timeout_seconds = 30.0
        startup_message = "Validator running. Press Ctrl+C to stop."
        idle_sleep_seconds = 1.0

        logging.getLogger("httpx").setLevel(logging.WARN)

        settings = cls._load_settings_or_exit(settings_class)
        validator = cls(settings)

        with validator.start_runtime(shutdown_timeout_seconds=shutdown_timeout_seconds):
            print(startup_message)
            try:
                while True:
                    time.sleep(idle_sleep_seconds)
            except KeyboardInterrupt:
                pass

    def connect[T](self, source: Source[T], sink: Sink[T]) -> None:
        """Connect two endpoints and register the owners of connected components."""
        self.subnet_flow.sources.add(source)
        self.subnet_flow.sinks.add(sink)
        self.subnet_flow.pipes[source].add(sink)
        self._register_endpoint_owner(source)
        self._register_endpoint_owner(sink)

    def _register_endpoint_owner(self, endpoint: Source[Any] | Sink[Any]) -> None:
        owner_node = endpoint.owner_node
        if owner_node is not None:
            self._connected_nodes[id(owner_node)] = owner_node

        owner_task = endpoint.owner_task
        if owner_task is not None:
            self._connected_tasks[id(owner_task)] = owner_task

    def _build_runtime(self) -> SubnetRuntime:
        connected_tasks = tuple(self._connected_tasks.values())
        for task in connected_tasks:
            self.connect(self.subnet_clock.source, task.block_beat)

        task_flows = [task.internal_flow for task in connected_tasks]

        all_nodes = [
            *self._connected_nodes.values(),
            *(node for task in connected_tasks for node in task.internal_nodes()),
        ]
        deduped_nodes = tuple({id(node): node for node in all_nodes}.values())

        return SubnetBuilder(nodes=deduped_nodes).add_flows(self.subnet_flow).add_flows(*task_flows).build()

    @contextmanager
    def start_runtime(self, shutdown_timeout_seconds: float = 30.0) -> Generator[SubnetRuntime]:
        if self.runtime is not None:
            raise RuntimeError("Runtime already started")
        self.runtime = self._build_runtime()
        with self.runtime.running(shutdown_timeout_seconds=shutdown_timeout_seconds) as runtime:
            yield runtime

    @staticmethod
    def _load_settings_or_exit[SettingsModel: BaseSettings](settings_class: type[SettingsModel]) -> SettingsModel:
        try:
            return settings_class()  # type: ignore[call-arg]
        except ValidationError as e:
            fields = ", ".join(str(err["loc"][-1]) for err in e.errors() if err.get("loc"))
            log.error(f"Configuration error: missing or invalid fields: {fields}")
            log.error("Check your .env file or environment variables.")
            sys.exit(1)
