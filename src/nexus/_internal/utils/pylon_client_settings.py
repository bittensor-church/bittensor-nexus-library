from pydantic import BaseModel, ConfigDict, Field, model_validator

from nexus._internal.utils.exceptions import SubnetMisconfiguredException


class PylonClientSettingsMixin(BaseModel):
    """Shared validator settings for creating a Pylon client."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    pylon_service_address: str = Field(validation_alias="VALIDATOR_PYLON_SERVICE_ADDRESS")
    pylon_open_access_token: str = Field(validation_alias="VALIDATOR_PYLON_OPEN_ACCESS_TOKEN")
    pylon_identity_name: str | None = Field(validation_alias="VALIDATOR_PYLON_IDENTITY_NAME", default=None)
    pylon_identity_token: str | None = Field(validation_alias="VALIDATOR_PYLON_IDENTITY_TOKEN", default=None)
    mtls_cert_path: str | None = Field(validation_alias="VALIDATOR_MTLS_CERT_PATH", default=None)
    mtls_key_path: str | None = Field(validation_alias="VALIDATOR_MTLS_KEY_PATH", default=None)
    neurons_file: str | None = Field(validation_alias="VALIDATOR_NEURONS_FILE", default=None)
    # Seconds an idle connection to a neuron stays pooled before being closed; reused across get_neuron_client calls.
    neuron_keepalive_expiry: float = Field(
        validation_alias="VALIDATOR_NEURON_CONNECTION_KEEPALIVE_EXPIRY", default=60.0
    )

    @model_validator(mode="after")
    def check_identity_fields(self) -> PylonClientSettingsMixin:
        if (self.pylon_identity_name is None) != (self.pylon_identity_token is None):
            raise SubnetMisconfiguredException(
                "Pylon identity configuration must provide both name and token. "
                "Expected VALIDATOR_PYLON_IDENTITY_NAME together with VALIDATOR_PYLON_IDENTITY_TOKEN."
            )
        return self
