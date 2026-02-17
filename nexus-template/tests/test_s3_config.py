# pyright: basic

from nexus.actors.s3_config import S3Config


def test_s3_config_allows_explicit_values() -> None:
    config = S3Config(
        bucket_name="uploads",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
        region_name="us-east-1",
        endpoint_url="http://localhost:9000",
        s3_addressing_style="path",
    )

    assert config.bucket_name == "uploads"
    assert config.aws_access_key_id == "test-access-key"
    assert config.aws_secret_access_key == "test-secret-key"
    assert config.region_name == "us-east-1"
    assert config.endpoint_url == "http://localhost:9000"
    assert config.s3_addressing_style == "path"


def test_s3_config_loads_values_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("NEXUS_S3_BUCKET_NAME", "uploads-env")
    monkeypatch.setenv("NEXUS_S3_AWS_ACCESS_KEY_ID", "env-access-key")
    monkeypatch.setenv("NEXUS_S3_AWS_SECRET_ACCESS_KEY", "env-secret-key")
    monkeypatch.setenv("NEXUS_S3_REGION_NAME", "us-west-2")
    monkeypatch.setenv("NEXUS_S3_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("NEXUS_S3_ADDRESSING_STYLE", "virtual")

    config = S3Config()  # pyright: ignore[reportCallIssue]

    assert config.bucket_name == "uploads-env"
    assert config.aws_access_key_id == "env-access-key"
    assert config.aws_secret_access_key == "env-secret-key"
    assert config.region_name == "us-west-2"
    assert config.endpoint_url == "http://localhost:9000"
    assert config.s3_addressing_style == "virtual"
