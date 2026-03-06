import threading
import uuid
from datetime import UTC, datetime

from cat_images.facilitator.models import Job, RegisteredValidator, StatusUpdate
from cat_images.facilitator.types import JobId, ValidatorHotkey
from cat_images.subnet_models import SingleCatImageInput


class ValidatorStore:
    def __init__(self, validators: dict[str, str] | None = None) -> None:
        self._lock = threading.Lock()
        self._validators: dict[ValidatorHotkey, RegisteredValidator] = {}
        # Pre-populate from config
        for hotkey, url in (validators or {}).items():
            self._validators[ValidatorHotkey(hotkey)] = RegisteredValidator(
                hotkey=ValidatorHotkey(hotkey), job_submission_url=url
            )

    def get(self, hotkey: ValidatorHotkey) -> RegisteredValidator | None:
        with self._lock:
            return self._validators.get(hotkey)

    def list_available(self) -> list[RegisteredValidator]:
        with self._lock:
            return [v for v in self._validators.values() if v.available]

    def list_all(self) -> list[RegisteredValidator]:
        with self._lock:
            return list(self._validators.values())

    def mark_unavailable(self, hotkey: ValidatorHotkey) -> None:
        with self._lock:
            if v := self._validators.get(hotkey):
                self._validators[hotkey] = v.model_copy(update={"available": False})

    def mark_available(self, hotkey: ValidatorHotkey) -> None:
        with self._lock:
            if v := self._validators.get(hotkey):
                self._validators[hotkey] = v.model_copy(update={"available": True})

    def toggle(self, hotkey: ValidatorHotkey) -> RegisteredValidator | None:
        with self._lock:
            if v := self._validators.get(hotkey):
                updated = v.model_copy(update={"available": not v.available})
                self._validators[hotkey] = updated
                return updated
            return None

    def delete(self, hotkey: ValidatorHotkey) -> bool:
        with self._lock:
            return self._validators.pop(hotkey, None) is not None


class JobStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[JobId, Job] = {}

    def create(self, job_spec: SingleCatImageInput, validator_hotkey: ValidatorHotkey, image_key: str) -> Job:
        job = Job(
            id=JobId(uuid.uuid4().hex[:12]),
            image_key=image_key,
            job_spec=job_spec,
            validator_hotkey=validator_hotkey,
            created_at=datetime.now(UTC),
        )
        with self._lock:
            self._jobs[job.id] = job
        return job

    def get(self, job_id: JobId) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_all(self) -> list[Job]:
        with self._lock:
            return list(reversed(self._jobs.values()))

    def append_status(self, job_id: JobId, update: StatusUpdate) -> bool:
        """Append a status update. Returns False if job not found or already terminal."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.is_terminal:
                return False
            job.status_updates.append(update)
            return True

    def status_count(self, job_id: JobId) -> int:
        """Return current number of status updates for a job."""
        with self._lock:
            job = self._jobs.get(job_id)
            return len(job.status_updates) if job else 0
