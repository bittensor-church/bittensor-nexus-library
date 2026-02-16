import json
import socket
import time
from threading import Thread
from urllib import error, request

from cat_images.validator import SingleCatImageInput, Validator


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _is_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _wait_for_port_state(*, port: int, should_be_open: bool, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _is_port_open(port) is should_be_open:
            return
        time.sleep(0.05)

    expected_state = "open" if should_be_open else "closed"
    raise AssertionError(f"Port {port} did not become {expected_state} within {timeout}s")


def _post_json(url: str, payload: dict[str, str]) -> tuple[int, str]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=2.0) as response:
            return response.status, response.read().decode("utf-8")
    except error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")


def test_validator_integration() -> None:
    port = _find_free_port()
    validator = Validator(port)
    url = f"http://127.0.0.1:{validator.entry.port}{validator.entry.path}"

    jobs: tuple[Thread, ...] = ()

    try:
        jobs = validator.run_loop()
        _wait_for_port_state(port=validator.entry.port, should_be_open=True)

        invalid_status, invalid_body = _post_json(url, {"image_s3_url": "a"})
        assert invalid_status == 400
        assert "Invalid request body" in invalid_body

        even_payload = {"image_s3_url": "aa", "image_name": "b"}
        even_status, even_body = _post_json(url, even_payload)
        assert even_status == 200
        assert "even number of characters" in even_body

        odd_payload = {"image_s3_url": "a", "image_name": "b"}
        odd_status, odd_body = _post_json(url, odd_payload)
        assert odd_status == 200
        expected_odd_result = str(SingleCatImageInput.model_validate(odd_payload)).upper()
        assert odd_body == expected_odd_result
    finally:
        validator.stop()
        cleanup_errors = []
        for job in jobs:
            try:
                job.join(5.0)
            except RuntimeError as exc:
                cleanup_errors.append(exc)
        for job in jobs:
            if job.is_alive():
                cleanup_errors.append(RuntimeError(f"Job {job.name} is still alive after join attempt"))
        try:
            _wait_for_port_state(port=validator.entry.port, should_be_open=False)
        except AssertionError as exc:
            cleanup_errors.append(RuntimeError(f"Port {validator.entry.port} is still open after stop attempt", exc))
        if cleanup_errors:
            raise ExceptionGroup("Errors during cleanup", cleanup_errors)