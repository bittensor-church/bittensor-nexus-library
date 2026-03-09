from nexus.actors import ExecutorCommunicator, NeuronRouter, Routed, TimestamperNode
from nexus.actors.chain_beat.block_beat import BlockBeat
from nexus.actors.executor_communicator import ProcessedInput
from nexus.actors.mux import Mux2
from nexus.actors.payload_creator import PayloadCreator
from nexus.actors.retry_strategy import RetriesExhaustedException, RetryStrategy
from nexus.actors.task_result_preparer import TaskResultPreparer
from nexus.actors.task_result_splitter import TaskResultSplitter
from nexus.actors.task_result_store_provider import DEFAULT_TASK_RESULT_STORE_PROVIDER, TaskResultStoreProvider
from nexus.actors.task_result_storer import TaskResultStorer
from nexus.core.dsl.flow import Flow
from nexus.core.dsl.nodes import Node, NodeId, NodeSinks, NodeSources, Sink, SinkName, Source, SourceName
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.task_result_store import SingleTaskResult
from nexus.utils.exceptions import NexusException


class NexusTask[Input, ExecutorPayload, ExecutorOutput, ExecutorPublicOutput = ExecutorOutput]:
    """Reusable task pipeline with split outputs for persisted task results and raw executor outputs."""

    name: NexusTaskName
    input: Sink[Input]
    block_beat: Sink[BlockBeat]
    task_result: Source[SingleTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]
    executor_output: Source[ExecutorPublicOutput | NexusException]
    error: Source[NexusException]
    internal_flow: Flow

    timestamper: TimestamperNode[
        Routed[ExecutorPayload],
        ProcessedInput[Routed[ExecutorPayload], ExecutorOutput],
    ]
    retry: RetryStrategy[Input]
    payload_creator: PayloadCreator[Input, ExecutorPayload]
    router: NeuronRouter[ExecutorPayload]
    executor_communicator: ExecutorCommunicator[ExecutorPayload, ExecutorOutput]
    task_result_preparer: TaskResultPreparer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
    task_result_storer: TaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
    task_result_splitter: TaskResultSplitter[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]
    executor_result_converter: PayloadCreator[ExecutorOutput, ExecutorPublicOutput]
    error_mux: Mux2[NexusException, RetriesExhaustedException, NexusException]

    def __init__(
        self,
        *,
        name: NexusTaskName,
        retry: RetryStrategy[Input],
        payload_creator: PayloadCreator[Input, ExecutorPayload],
        router: NeuronRouter[ExecutorPayload],
        executor_communicator: ExecutorCommunicator[ExecutorPayload, ExecutorOutput],
        executor_result_converter: PayloadCreator[ExecutorOutput, ExecutorPublicOutput],
        task_result_store_provider: TaskResultStoreProvider[
            ExecutorPayload,
            ExecutorOutput,
            ExecutorPublicOutput,
        ]
        | None = None,
    ) -> None:
        self.name = name
        self.timestamper = TimestamperNode[
            Routed[ExecutorPayload],
            ProcessedInput[Routed[ExecutorPayload], ExecutorOutput],
        ](f"{name}-timestamper")
        self.retry = retry
        self.payload_creator = payload_creator
        self.router = router
        self.executor_communicator = executor_communicator
        self.executor_result_converter = executor_result_converter
        self.task_result_preparer = TaskResultPreparer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
            _id=NodeId(f"{name}-task-result-preparer")
        )
        if task_result_store_provider is None:
            task_result_store_provider = DEFAULT_TASK_RESULT_STORE_PROVIDER
        self.task_result_storer = TaskResultStorer[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
            _id=NodeId(f"{name}-task-result-storer"),
            name=name,
            task_result_store_provider=task_result_store_provider,
        )
        self.task_result_splitter = TaskResultSplitter[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
            _id=NodeId(f"{name}-task-result-splitter")
        )
        self.error_mux = Mux2[NexusException, RetriesExhaustedException, NexusException](
            _id=NodeId(f"{name}-error-mux")
        )

        self.block_beat = self.timestamper.block_beat
        self.input = self.retry.input
        self.task_result = self.task_result_splitter.task_result
        self.executor_output = self.task_result_splitter.executor_output
        self.error = self.error_mux.out
        for endpoint in (self.block_beat, self.input, self.task_result, self.executor_output, self.error):
            endpoint.owner_task = self

        self.internal_flow = Flow(
            entry_sinks=NodeSinks(
                sinks={SinkName("input"): self.input},
            ),
            exit_sources=NodeSources(
                sources={
                    SourceName("task_result"): self.task_result,
                    SourceName("executor_output"): self.executor_output,
                },
            ),
        )
        self.internal_flow.sinks.add(self.input)
        self.internal_flow.sources.add(self.task_result)
        self.internal_flow.sources.add(self.executor_output)

        def connect[T](source: Source[T], sink: Sink[T]) -> None:
            self.internal_flow.sources.add(source)
            self.internal_flow.sinks.add(sink)
            self.internal_flow.pipes[source].add(sink)

        connect(self.retry.next_attempt, self.payload_creator.input)
        connect(self.payload_creator.created_payload, self.router.input)
        connect(self.router.routed, self.timestamper.input)
        connect(self.timestamper.forwarded_input, self.executor_communicator.input)
        connect(self.executor_communicator.processed, self.timestamper.executor_output)
        connect(self.timestamper.timestamped_output, self.task_result_preparer.timestamped_result)
        connect(self.task_result_preparer.executor_output_for_conversion, self.executor_result_converter.input)
        connect(self.executor_result_converter.created_payload, self.task_result_preparer.converted_public_output)
        connect(self.task_result_preparer.prepared_task_result, self.task_result_storer.sink)
        connect(self.task_result_storer.task_result, self.task_result_splitter.task_result_input)
        connect(self.executor_communicator.error, self.retry.failed_attempt)
        connect(self.task_result_storer.error, self.retry.failed_attempt)
        connect(self.payload_creator.error, self.retry.failed_attempt)
        connect(self.router.error, self.retry.failed_attempt)
        connect(self.retry.error, self.error_mux.left)
        connect(self.task_result_preparer.error, self.error_mux.right)
        connect(self.executor_result_converter.error, self.task_result_preparer.conversion_failed)
        connect(self.executor_result_converter.error, self.error_mux.right)

    def internal_nodes(self) -> tuple[Node, ...]:
        """Return all internal nodes in build order for `SubnetBuilder(nodes=...)`."""
        return (
            self.retry,
            self.payload_creator,
            self.router,
            self.timestamper,
            self.executor_communicator,
            self.task_result_preparer,
            self.executor_result_converter,
            self.task_result_storer,
            self.task_result_splitter,
            self.error_mux,
        )
