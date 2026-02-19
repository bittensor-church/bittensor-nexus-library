from abc import ABC, abstractmethod
from typing import override

import boto3


class S3ClientProvider(ABC):
    @abstractmethod
    def get_client(self):
        ...

class DefaultS3ClientProvider(S3ClientProvider):
    @override
    def get_client(self):
        return boto3.client("s3")