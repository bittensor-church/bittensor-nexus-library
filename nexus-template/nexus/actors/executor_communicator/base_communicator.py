from typing import override

from nexus.core.dsl.nodes import NodeSinks, NodeSources, Sink, SinkName, Source, SourceName, Transform


class ExecutorCommunicator[Input, Output](Transform[Input, Output]):
    """
    Transport-agnostic contract for executor-facing communicators.

    An executor communicator bridges routed execution requests and executor results
    in the processing graph:
    - consumes request payloads on `input`
    - emits successful executor outputs on `processed`
    - emits failures on `error`

    This class defines only the logical node interface and naming conventions.
    Concrete implementations provide transport/protocol details. The current codebase
    includes an async HTTP implementation (`AsyncHttpNeuronCommunicator`), but the
    same contract can be implemented for other transports such as sync HTTP,
    WebSocket, or RPC-based protocols.
    """

    input: Sink[Input]
    processed: Source[Output]

    def __init__(
        self,
        _id: str,
    ) -> None:
        super().__init__(_id)

        # alias for convenience
        self.input = self.sink
        self.processed = self.ok

    @override
    def sinks(self) -> NodeSinks:
        return NodeSinks(sinks={SinkName("input"): self.input})

    @override
    def sources(self) -> NodeSources:
        return NodeSources(
            sources={
                SourceName("processed"): self.processed,
                SourceName("error"): self.error,
            },
            default_source=self.processed,
        )
