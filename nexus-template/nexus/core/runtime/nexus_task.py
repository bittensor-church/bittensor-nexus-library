from nexus.actors import ExecutorCommunicator, NeuronRouter, Routed, TimestamperNode
from nexus.actors.chain_beat.block_beat import BlockBeat
from nexus.actors.executor_communicator import ProcessedInput
from nexus.actors.payload_creator import PayloadCreator
from nexus.actors.retry_strategy import RetriesExhaustedException, RetryStrategy
from nexus.actors.task_result_store_provider import DefaultTaskResultStoreProvider, TaskResultStoreProvider
from nexus.actors.task_result_storer import TaskResultStorer
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Node, NodeId, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus.core.runtime.nexus_task_types import NexusTaskName, TaskResultId


class NexusTask[Input, Output, ExecutorPayload]:
    """Reusable task pipeline composed of retry, payload creation, routing, execution, and result storing."""

    name: NexusTaskName
    input: Sink[Input]
    block_beat: Sink[BlockBeat]
    output: Source[TaskResultId]
    error: Source[RetriesExhaustedException]
    internal_flow: Flow

    timestamper: TimestamperNode[
        Routed[ExecutorPayload],
        ProcessedInput[Routed[ExecutorPayload], Output],
    ]
    retry: RetryStrategy[Input]
    payload_creator: PayloadCreator[Input, ExecutorPayload]
    router: NeuronRouter[ExecutorPayload]
    executor_communicator: ExecutorCommunicator[ExecutorPayload, Output]
    task_result_storer: TaskResultStorer[ExecutorPayload, Output]

    def __init__(
        self,
        *,
        name: NexusTaskName,
        retry: RetryStrategy[Input],
        payload_creator: PayloadCreator[Input, ExecutorPayload],
        router: NeuronRouter[ExecutorPayload],
        executor_communicator: ExecutorCommunicator[ExecutorPayload, Output],
        task_result_store_provider: TaskResultStoreProvider[ExecutorPayload, Output] | None = None,
    ) -> None:
        self.name = name
        self.timestamper = TimestamperNode[
            Routed[ExecutorPayload],
            ProcessedInput[Routed[ExecutorPayload], Output],
        ](f"{name}-timestamper")
        self.retry = retry
        self.payload_creator = payload_creator
        self.router = router
        self.executor_communicator = executor_communicator
        if task_result_store_provider is None:
            task_result_store_provider = DefaultTaskResultStoreProvider[ExecutorPayload, Output]()
        self.task_result_storer = TaskResultStorer[ExecutorPayload, Output](
            _id=NodeId(f"{name}-task-result-storer"),
            name=name,
            task_result_store_provider=task_result_store_provider,
        )

        self.block_beat = self.timestamper.block_beat
        self.input = self.retry.input
        self.output = self.task_result_storer.task_result_ids
        self.error = self.retry.error

        self.internal_flow = Flow(
            entry_sinks=NodeSinks(
                sinks={SinkName("input"): self.input},
            ),
            exit_sources=NodeSources(
                sources={SourceName("output"): self.output},
            ),
        )
        self.internal_flow.sinks.add(self.input)
        self.internal_flow.sources.add(self.output)

        def connect[T](source: Source[T], sink: Sink[T]) -> None:
            self.internal_flow.sources.add(source)
            self.internal_flow.sinks.add(sink)
            self.internal_flow.pipes[source].add(sink)

        connect(self.retry.next_attempt, self.payload_creator.input)
        connect(self.payload_creator.created_payload, self.router.input)
        connect(self.router.routed, self.timestamper.input)
        connect(self.timestamper.forwarded_input, self.executor_communicator.input)
        connect(self.executor_communicator.processed, self.timestamper.executor_output)
        connect(self.timestamper.timestamped_output, self.task_result_storer.sink)
        connect(self.executor_communicator.error, self.retry.failed_attempt)
        connect(self.task_result_storer.error, self.retry.failed_attempt)
        connect(self.payload_creator.error, self.retry.failed_attempt)
        connect(self.router.error, self.retry.failed_attempt)

    def internal_nodes(self) -> tuple[Node, ...]:
        """Return all internal nodes in build order for `SubnetBuilder(nodes=...)`."""
        return (
            self.retry,
            self.payload_creator,
            self.router,
            self.timestamper,
            self.executor_communicator,
            self.task_result_storer,
        )
