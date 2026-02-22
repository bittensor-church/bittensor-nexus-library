# pyright: basic

import os
from collections.abc import Iterator
from unittest.mock import patch

import boto3
import pytest
from moto.server import ThreadedMotoServer

from nexus.utils.types import SubnetId

DEFAULT_TEST_S3_BUCKET = "uploads"
DEFAULT_TEST_NETUID = SubnetId(1)


@pytest.fixture
def default_test_s3_bucket() -> str:
    return DEFAULT_TEST_S3_BUCKET


@pytest.fixture
def default_test_netuid() -> SubnetId:
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
