# pyright: basic

import os
from collections.abc import Iterator
from unittest.mock import patch

import boto3
import pytest
from moto.server import ThreadedMotoServer
from transform_test_utils import (
    TransformActorTestSetup,
    TransformActorTestSetupFactory,
    build_runtime,
)

from nexus.core.dsl.nodes import Transform
from nexus.utils.types import NetUid

DEFAULT_TEST_S3_BUCKET = "uploads"
DEFAULT_TEST_NETUID = NetUid(1)


@pytest.fixture
def default_test_s3_bucket() -> str:
    return DEFAULT_TEST_S3_BUCKET


@pytest.fixture
def default_test_netuid() -> NetUid:
    return DEFAULT_TEST_NETUID


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
