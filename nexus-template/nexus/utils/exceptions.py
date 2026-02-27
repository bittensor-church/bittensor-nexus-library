class NexusException(Exception):
    """Base exception for all Nexus errors."""

    pass


class InternalStateCorruptionException(NexusException):
    """Raised when an internal state corruption is detected."""

    pass


class SafeInvokeWrappedException(NexusException):
    """Raised when an unexpected exception is caught during safe_invoke
    and wrapped for propagation."""

    pass


class NoRoutableNeuronsException(NexusException):
    """Raised when we cannot find any routable neurons."""

    pass


class InternalFrameworkException(NexusException):
    """Raised when an unexpected error occurs within the framework itself,
    indicating a potential bug."""

    pass


class FlowMisconfiguredException(NexusException):
    """Raised when a flow looks misconfigured, e.g. when invalid
    sinks are being connected etc."""

    pass


class ActorMisconfiguredException(NexusException):
    """Raised when an actor is misconfigured, e.g. when its specification
    is invalid."""

    pass
