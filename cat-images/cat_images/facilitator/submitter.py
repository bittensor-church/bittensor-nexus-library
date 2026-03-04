import logging

import httpx

from cat_images.facilitator.models import RegisteredValidator
from cat_images.subnet import SingleCatImageInput, ValidatorResult

log = logging.getLogger("facilitator.submitter")


class SubmissionError(Exception):
    pass


class JobSubmitter:
    """Sends raw SingleCatImageInput to a validator and waits for the synchronous response."""

    def __init__(self, max_retries: int = 3, timeout: float = 30.0) -> None:
        self._max_retries = max_retries
        self._timeout = timeout

    def submit(self, validator: RegisteredValidator, job_spec: SingleCatImageInput) -> ValidatorResult:
        """Blocking call — POST job_spec to validator, return parsed result."""
        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                with httpx.Client(timeout=self._timeout) as client:
                    resp = client.post(
                        validator.job_submission_url,
                        json=job_spec.model_dump(mode="json"),
                    )
                    resp.raise_for_status()
                    return ValidatorResult.model_validate(resp.json())
            except Exception as e:
                last_error = e
                log.warning(f"Submission attempt {attempt}/{self._max_retries} failed: {e}")

        raise SubmissionError(f"Failed after {self._max_retries} attempts: {last_error}")
