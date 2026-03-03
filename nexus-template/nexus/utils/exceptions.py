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


class ExecutorFailureException(NexusException):
    """Raised when executor fails while handling a specific input.

    We persist context payloads using deep-copy/pickling paths. Exception
    reconstruction for custom exceptions relies on constructor arguments, so we
    implement ``__reduce__`` to restore this type with ``(executor_error)``.
    """

    executor_error: NexusException

    def __init__(self, executor_error: NexusException) -> None:
        super().__init__("Executor failed to process input")
        self.executor_error = executor_error

    def __reduce__(self) -> tuple[type[ExecutorFailureException], tuple[NexusException]]:
        return type(self), (self.executor_error,)


class UnsupportedAxonProtocolException(NexusException):
    """Raised when an operation expects one axon protocol but receives another one.

    Stores both `expected_protocol` and `actual_protocol` for downstream handling.
    """

    expected_protocol: object | None
    actual_protocol: object | None

    def __init__(
        self,
        message: str | None = None,
        *,
        expected_protocol: object | None = None,
        actual_protocol: object | None = None,
    ) -> None:
        self.expected_protocol = expected_protocol
        self.actual_protocol = actual_protocol
        if message is None:
            if expected_protocol is None and actual_protocol is None:
                message = "Unsupported axon protocol."
            else:
                message = f"Unsupported axon protocol: expected={expected_protocol!r}, actual={actual_protocol!r}."
        super().__init__(message)


class AsyncHttpNeuronCommunicatorException(NexusException):
    """Base class for failures specific to AsyncHttpNeuronCommunicator."""

    pass


class NeuronAddressInvalidException(AsyncHttpNeuronCommunicatorException):
    """Raised when target neuron HTTP address data (IP/port) is invalid."""

    pass


class RemoteRequestFailedException(AsyncHttpNeuronCommunicatorException):
    """Raised when sending the outbound HTTP request fails due to network/transport issues."""

    pass


class RemoteRequestRejectedException(AsyncHttpNeuronCommunicatorException):
    """Raised when target service responds to the outbound request with non-2xx HTTP status."""

    pass


class RemoteResponseTimeoutException(AsyncHttpNeuronCommunicatorException):
    """Raised when no callback response is received before total processing timeout."""

    pass


class ResponseInvalidException(AsyncHttpNeuronCommunicatorException):
    """Raised when callback message is protocol-invalid, e.g. missing both output and error."""

    pass


class ResponseValidationException(AsyncHttpNeuronCommunicatorException):
    """Raised when callback has output data, but that output fails expected Output model validation."""

    pass


class RemoteExecutionException(AsyncHttpNeuronCommunicatorException):
    """Raised when remote service explicitly reports execution failure via callback error field."""

    pass


class WeightSettingException(NexusException):
    """Raised when the weight setting actor fails when executing the weighing
    function or has trouble reaching pylon"""

    pass


class RetryTaskAfterExecutorFailureException(NexusException):
    """Raised by task result storer to indicate that a task should be retried after an executor failure."""

    pass


class EmbeddedExecutorFailureException(NexusException):
    """Raised when an exception happens during execution in embedded executor."""

    pass