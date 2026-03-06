"""Internal domain models and persistable entities.
Job specs, routing requests, status tracking, and the Job aggregate itself."""

from datetime import datetime

from pydantic import BaseModel

from cat_images.facilitator.types import JobId, JobLiveness, ValidatorHotkey
from cat_images.subnet_models import UserImageInput, ValidatorResult


class RegisteredValidator(BaseModel):
    hotkey: ValidatorHotkey
    job_submission_url: str
    available: bool = True


class CatificationRequest(BaseModel):
    """Inbound request before routing. Carries the spec and future metadata
    like validator preference."""

    job_spec: UserImageInput


class StatusUpdate(BaseModel):
    """Internal representation with faci-side timestamp."""

    status: str
    liveness: JobLiveness
    validator_timestamp: datetime
    received_at: datetime
    result: ValidatorResult | None = None


class Job(BaseModel):
    id: JobId
    image_key: str  # S3 object key for direct download by proxy
    job_spec: UserImageInput
    validator_hotkey: ValidatorHotkey
    status_updates: list[StatusUpdate] = []
    created_at: datetime

    @property
    def liveness(self) -> JobLiveness:
        return self.status_updates[-1].liveness if self.status_updates else JobLiveness.IN_PROGRESS

    @property
    def is_terminal(self) -> bool:
        return self.liveness.is_terminal

    @property
    def result(self) -> ValidatorResult | None:
        if self.status_updates and self.liveness == JobLiveness.SUCCESS:
            return self.status_updates[-1].result
        return None

    @property
    def latest_status(self) -> str:
        return self.status_updates[-1].status if self.status_updates else "Submitted"
