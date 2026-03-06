"""Background job processor. Sends jobs to validators in a thread pool
and updates job status with progress messages while waiting."""

import logging
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from cat_images.facilitator.models import RegisteredValidator, StatusUpdate
from cat_images.facilitator.stores import JobStore
from cat_images.facilitator.submitter import JobSubmitter, SubmissionError
from cat_images.facilitator.types import JobId, JobLiveness
from cat_images.subnet_models import SingleCatImageInput, ValidatorResult

log = logging.getLogger("facilitator.worker")

_WAITING_MESSAGES = [
    "Teaching the cat to hold a paintbrush...",
    "Negotiating with a very stubborn tabby...",
    "Cat knocked the image off the table, retrieving...",
    "Untangling yarn from the GPU...",
    "Cat is napping on the keyboard, please wait...",
    "Bribing cat with treats to cooperate...",
    "Cat demands a belly rub before continuing...",
    "Chasing a laser pointer for inspiration...",
    "Cat is judging your image... silently...",
    "Removing cat hair from the neural network...",
    "Cat sat on the submit button again...",
    "Consulting the ancient feline oracle...",
    "Cat is doing zoomies across the server room...",
    "Warming up the purr engine...",
    "Cat found a box and refuses to leave it...",
]


def _append(
    job_store: JobStore, job_id: JobId, status: str, liveness: JobLiveness, result: ValidatorResult | None = None
) -> None:
    now = datetime.now(UTC)
    job_store.append_status(
        job_id,
        StatusUpdate(
            status=status,
            liveness=liveness,
            validator_timestamp=now,
            received_at=now,
            result=result,
        ),
    )


class JobWorker:
    def __init__(self, job_store: JobStore, submitter: JobSubmitter, max_workers: int = 4) -> None:
        self._job_store = job_store
        self._submitter = submitter
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="job-worker")

    def dispatch(self, job_id: JobId, job_spec: SingleCatImageInput, validator: RegisteredValidator) -> None:
        self._pool.submit(self._run, job_id, job_spec, validator)

    def _run(self, job_id: JobId, job_spec: SingleCatImageInput, validator: RegisteredValidator) -> None:
        _append(self._job_store, job_id, "Sent to validator, waiting...", JobLiveness.IN_PROGRESS)

        # Silly ticker that posts a random cat message every second
        # In reality we will want the validator to push status updates to some dedicated endpoint and use the same
        # job status log, but that's not there yet, so the silly messages must do.
        stop_ticker = threading.Event()
        ticker = threading.Thread(
            target=self._tick,
            args=(job_id, stop_ticker),
            daemon=True,
            name=f"ticker-{job_id}",
        )
        ticker.start()

        try:
            result = self._submitter.submit(validator, job_spec)
            stop_ticker.set()
            ticker.join(timeout=2)
            _append(self._job_store, job_id, "Done!", JobLiveness.SUCCESS, result=result)
        except SubmissionError as e:
            stop_ticker.set()
            ticker.join(timeout=2)
            log.error(f"Job {job_id} failed: {e}")
            _append(self._job_store, job_id, f"Failed: {e}", JobLiveness.FAILED)

    def _tick(self, job_id: JobId, stop: threading.Event) -> None:
        messages = list(_WAITING_MESSAGES)
        random.shuffle(messages)
        idx = 0
        while not stop.wait(timeout=2.0):
            msg = messages[idx % len(messages)]
            idx += 1
            _append(self._job_store, job_id, msg, JobLiveness.IN_PROGRESS)
