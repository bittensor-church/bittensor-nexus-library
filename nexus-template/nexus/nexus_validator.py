# pyright: basic
from contextlib import contextmanager
from typing import Any, Generator

from pydantic_settings import BaseSettings

from nexus.actors import PylonClientProvider, StaticConfigPylonClientProvider, BlockBeatNode
from nexus.actors.task_result_store_provider import TaskResultStoreProvider, DefaultTaskResultStoreProvider
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import NodeSinks, NodeSources, Source, Sink, SourceName, Node
from nexus.core.runtime.nexus_task import NexusTask
from nexus.core.runtime.subnet_runtime import SubnetRuntime, SubnetBuilder


class NexusValidator:
    pylon_client_provider: PylonClientProvider
    task_result_store_provider: TaskResultStoreProvider[Any, Any, Any]
    subnet_clock: BlockBeatNode

    nodes: list[Node]
    tasks: list[NexusTask]
    subnet_flow: Flow
    runtime: SubnetRuntime | None = None

    def __init__(self, settings: BaseSettings) -> None:
        map = settings.model_dump()
        self.pylon_client_provider = StaticConfigPylonClientProvider(
            pylon_service_address=map["pylon_service_address"],
            open_access_token=map["pylon_open_access_token"],)

        self.task_result_store_provider = DefaultTaskResultStoreProvider()

        self.subnet_clock = BlockBeatNode("internal-subnet-clock", pylon_client_provider=self.pylon_client_provider)
        self.nodes = [self.subnet_clock]
        self.tasks = []

        self.subnet_flow = Flow(
            entry_sinks=NodeSinks(sinks={}),
            exit_sources=NodeSources(sources={
                SourceName("internal-subnet-clock"): self.subnet_clock.source
            }),
        )

    def connect[T](self, source: Source[T], sink: Sink[T]) -> None:
        self.subnet_flow.sources.add(source)
        self.subnet_flow.sinks.add(sink)
        self.subnet_flow.pipes[source].add(sink)


    def add_nodes(self, *nodes: Node | NexusTask) -> None:
        for node in nodes:
            if isinstance(node, NexusTask):
                self.tasks.append(node)
            else:
                self.nodes.append(node)

    def _build_runtime(self) -> SubnetRuntime:
        all_nodes = self.nodes[:]
        task_flows = []
        for task in self.tasks:
            self.connect(self.subnet_clock.source, task.block_beat)
            all_nodes.extend(task.internal_nodes())
            task_flows.append(task.internal_flow)
        return (
            SubnetBuilder(
                nodes=all_nodes
            )
            .add_flows(self.subnet_flow)
            .add_flows(*task_flows)
            .build()
        )

    @contextmanager
    def start_runtime(self, shutdown_timeout_seconds: float = 30.0) -> Generator[SubnetRuntime]:
        if self.runtime is not None:
            raise RuntimeError("Runtime already started")
        self.runtime = self._build_runtime()
        with self.runtime.running(shutdown_timeout_seconds=shutdown_timeout_seconds) as runtime:
            yield runtime
