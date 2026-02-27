from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Final, override

import boto3

if TYPE_CHECKING:
    # do not import stubs in production code, as they are not needed and may not be available
    from mypy_boto3_s3.client import S3Client


class S3ClientProvider(ABC):
    @abstractmethod
    def get_client(self) -> S3Client: ...


class DefaultS3ClientProvider(S3ClientProvider):
    @override
    def get_client(self) -> S3Client:
        return boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]


DEFAULT_S3_CLIENT_PROVIDER: Final[S3ClientProvider] = DefaultS3ClientProvider()
