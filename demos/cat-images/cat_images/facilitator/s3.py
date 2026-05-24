import uuid

import boto3
from botocore.config import Config

from cat_images.facilitator.settings import FacilitatorSettings

# 7 days — max allowed by S3
_PRESIGNED_URL_EXPIRY = 7 * 24 * 3600


class S3Client:
    def __init__(self, settings: FacilitatorSettings) -> None:
        addressing_style = "path" if settings.s3_endpoint_url else "virtual"
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region or None,
            config=Config(signature_version="s3v4", s3={"addressing_style": addressing_style}),
        )
        self._bucket = settings.s3_bucket

    def upload_image(self, data: bytes, content_type: str) -> str:
        """Upload image bytes, return the S3 object key."""
        key = f"uploads/{uuid.uuid4().hex}"
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data, ContentType=content_type)
        return key

    def presign(self, key: str) -> str:
        """Generate a presigned GET URL for a key."""
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": key},
            ExpiresIn=_PRESIGNED_URL_EXPIRY,
        )

    def download(self, key: str) -> tuple[bytes, str]:
        """Download an object, return (bytes, content_type)."""
        resp = self._client.get_object(Bucket=self._bucket, Key=key)
        content_type = resp.get("ContentType", "image/png")
        return resp["Body"].read(), content_type
