# pyright: basic

import pickle
import uuid

from pylon_client.artanis.v1 import AxonProtocol

from nexus.v1 import NexusTaskName, TaskResultId, TaskResultNotFoundException, UnsupportedAxonProtocolException


def test_unsupported_axon_protocol_exception_round_trips_through_pickle() -> None:
    original = UnsupportedAxonProtocolException(
        expected_protocol=AxonProtocol.HTTP,
        actual_protocol=AxonProtocol.TCP,
    )

    loaded = pickle.loads(pickle.dumps(original))

    assert isinstance(loaded, UnsupportedAxonProtocolException)
    assert loaded.expected_protocol == AxonProtocol.HTTP
    assert loaded.actual_protocol == AxonProtocol.TCP
    assert str(loaded) == str(original)


def test_task_result_not_found_exception_round_trips_through_pickle() -> None:
    task_name = NexusTaskName("pickle-task")
    task_result_id = TaskResultId(uuid.uuid4())
    original = TaskResultNotFoundException(
        task_name=task_name,
        task_result_id=task_result_id,
    )

    loaded = pickle.loads(pickle.dumps(original))

    assert isinstance(loaded, TaskResultNotFoundException)
    assert loaded.task_name == task_name
    assert loaded.task_result_id == task_result_id
    assert str(loaded) == str(original)
