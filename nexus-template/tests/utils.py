# pyright: basic
from threading import Thread

from tenacity import RetryError, retry, stop_after_delay, wait_fixed


def wait_until(condition, *, timeout=1.0, interval=0.05):
    """
        I wasn't able to find this as a library function :shrug:

        Wait until the given condition callable returns True, or raise an AssertionError if the timeout is reached.
    """  # noqa: DOC501

    @retry(stop=stop_after_delay(timeout), wait=wait_fixed(interval), reraise=True)
    def _check():
        if not condition():
            raise AssertionError("Condition not yet true")

    try:
        _check()
    except RetryError as exc:
        raise AssertionError(f"Condition not met within {timeout} seconds") from exc.last_attempt.exception()


class Jobs:
    def __init__(self, *jobs: Thread):
        self.jobs = jobs

    def join(self, timeout=1.0):
        for job in self.jobs:
            job.join(timeout)
            assert not job.is_alive()
