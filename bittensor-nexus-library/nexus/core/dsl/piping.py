from dataclasses import dataclass

from .flow import Flow
from .nodes import Pipes, Sinks, Sources


@dataclass
class Piping:
    """
    DSL for defining data flow connections between Sources and Sinks.
    """

    pipes: Pipes
    sources: Sources
    sinks: Sinks

    def __init__(self):
        self.pipes = Pipes(set)
        self.sources = set()
        self.sinks = set()

    def add_flow(self, flow: Flow):
        self.sources.update(flow.sources)
        self.sinks.update(flow.sinks)
        for source, sinks in flow.pipes.items():
            self.pipes[source].update(sinks)
