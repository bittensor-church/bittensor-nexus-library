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

