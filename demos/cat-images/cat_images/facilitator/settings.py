from pydantic_settings import BaseSettings, SettingsConfigDict


class FacilitatorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FACI_", env_file=".env", extra="ignore")

    s3_endpoint_url: str | None = None
    s3_bucket: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str = ""

    port: int = 8080
    host: str = "0.0.0.0"
    # Known validators: {"hotkey": "http://validator:port/submit", ...}
    validators: dict[str, str]

    submit_max_retries: int = 3
    submit_timeout_seconds: float = 30.0
