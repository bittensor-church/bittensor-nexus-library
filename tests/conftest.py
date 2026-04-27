# pyright: basic

import os
import socket
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from threading import Lock
from unittest.mock import patch

import boto3
import pytest
from moto.server import ThreadedMotoServer
from nexus_task_test_setup import (
    NexusTaskTestSetup,
    NexusTaskTestSetupFactory,
    build_nexus_task_test_setup,
)
from pylon_client.artanis import Port
from transform_test_utils import (
    TransformActorTestSetup,
    TransformActorTestSetupFactory,
    build_runtime,
)
from utils import DEFAULT_TEST_NETUID

from nexus.v1 import NetUid, Transform, subnet_settings_module

DEFAULT_TEST_S3_BUCKET = "uploads"


@pytest.fixture(autouse=True)
def _isolate_subnet_settings_between_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subnet_settings_module,
        "_subnet_settings_registry",
        subnet_settings_module._SubnetSettingsRegistry(),
    )


@dataclass
class PortBlockAllocator:
    """
    Session-scoped allocator that gives each test process a distinct local port range.

    Strategy:
    - try to bind a "guard" port at `base`, then `base + block_size`, and so on
    - successful guard bind claims that block for the process lifetime
    - hand out subsequent ports from that block (`guard + 1 .. guard + block_size - 1`)

    This reduces cross-process collisions for tests running in parallel.
    It does not guarantee every returned port is globally free at bind time.
    """

    base: int = 9000
    block_size: int = 100
    _guard_socket: socket.socket | None = None
    _next_port: int = field(init=False, default=0)
    _end_port: int = field(init=False, default=0)
    _lock: Lock = field(init=False, default_factory=Lock)

    def __post_init__(self) -> None:
        """Claim one block and initialize the monotonic per-process port sequence."""
        block_start = self._claim_block_start()
        self._next_port = block_start + 1
        self._end_port = block_start + self.block_size - 1

    def _claim_block_start(self) -> int:
        """Claim the first available block start by binding and keeping a guard socket."""
        block_start = self.base
        while True:
            guard_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                guard_socket.bind(("127.0.0.1", block_start))
            except OSError:
                guard_socket.close()
                block_start += self.block_size
                continue
            self._guard_socket = guard_socket
            return block_start

    def next_port(self) -> Port:
        """
        Return the next port in this process's claimed block.

        Raises:
            RuntimeError: if all ports in the claimed block were already allocated.

        """
        with self._lock:
            if self._next_port > self._end_port:
                raise RuntimeError("Allocated test port block exhausted.")
            allocated = self._next_port
            self._next_port += 1
            return Port(allocated)

    def close(self) -> None:
        """Release the guard socket and relinquish block ownership."""
        guard_socket = self._guard_socket
        self._guard_socket = None
        if guard_socket is not None:
            guard_socket.close()


@pytest.fixture
def default_test_s3_bucket() -> str:
    return DEFAULT_TEST_S3_BUCKET


@pytest.fixture
def default_test_netuid() -> NetUid:
    return DEFAULT_TEST_NETUID


@pytest.fixture(scope="session")
def port_block_allocator() -> Iterator[PortBlockAllocator]:
    allocator = PortBlockAllocator()
    try:
        yield allocator
    finally:
        allocator.close()


@pytest.fixture
def unused_local_port(port_block_allocator: PortBlockAllocator) -> Callable[[], Port]:
    return port_block_allocator.next_port


@pytest.fixture
def moto_s3_endpoint_url() -> Iterator[str]:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    try:
        host, port = server.get_host_and_port()
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
def default_s3_storage_client(
    moto_s3_endpoint_url: str,
    default_test_s3_bucket: str,
) -> Iterator:
    with patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "test-access-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret-key",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_ENDPOINT_URL_S3": moto_s3_endpoint_url,
        },
    ):
        admin_client = boto3.client("s3")
        admin_client.create_bucket(Bucket=default_test_s3_bucket)
        yield admin_client


@pytest.fixture
def transform_actor_test_setup_factory() -> TransformActorTestSetupFactory:
    def _build[Input, Output](transform: Transform[Input, Output]) -> TransformActorTestSetup[Input, Output]:
        runtime, processed_collector, error_collector, upstream_source = build_runtime(transform=transform)
        return TransformActorTestSetup(
            runtime=runtime,
            processed_collector=processed_collector,
            error_collector=error_collector,
            upstream_source=upstream_source,
        )

    return _build


@pytest.fixture
def nexus_task_test_setup_factory() -> NexusTaskTestSetupFactory:
    def _build(
        *,
        retry=None,
        payload_creator=None,
        router=None,
        executor_communicator=None,
    ) -> NexusTaskTestSetup:
        return build_nexus_task_test_setup(
            retry=retry,
            payload_creator=payload_creator,
            router=router,
            executor_communicator=executor_communicator,
        )

    return _build
