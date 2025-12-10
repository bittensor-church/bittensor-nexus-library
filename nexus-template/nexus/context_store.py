from dataclasses import dataclass


@dataclass
class ContextId:
    """
    Represents a unique identifier for a context in the Nexus system.
    """

    id: str
