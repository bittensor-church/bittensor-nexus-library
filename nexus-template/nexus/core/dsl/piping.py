from dataclasses import dataclass

from .nodes import Pipes, Sink, Sinks, Source, Sources


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

    def connect[T](self, source: Source[T], sink: Sink[T]) -> None:
        self.pipes[source].add(sink)
