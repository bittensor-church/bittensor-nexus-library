"""
Standalone script to test a running cat miner.

Spins up a local Litestar server that acts as:
  - source image host  (GET  /source)
  - fake S3 upload     (PUT  /upload)
  - result callback    (POST /callback)

Usage:
    uv run test_scripts/miner_test.py photo.png result.png
    uv run test_scripts/miner_test.py photo.png result.png --miner-url http://10.0.0.5:9090/process
"""

import logging
import socket
import sys
import time
import uuid
from pathlib import Path
from threading import Event, Thread

import click
import httpx
import uvicorn
from litestar import Controller, Litestar, Request, Response, get, post, put
from litestar.datastructures import State
from nexus.v1 import (
    AsyncHttpNeuronRequestEnvelope,
    AsyncHttpNeuronResponseEnvelope,
    RequestId,
)
from pydantic import AnyHttpUrl

from cat_images.miner import MinerInput

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s", datefmt="%H:%M:%S", level=logging.INFO
)
log = logging.getLogger("miner_test")


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_for_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"Server did not start on port {port} within {timeout}s")


class MinerTestController(Controller):
    """Litestar controller that fakes source hosting, S3 upload, and miner callback."""

    path = "/"

    @get("/source")
    async def serve_source(self, request: Request) -> Response:
        backend: MinerTestBackend = request.app.state.backend
        return Response(content=backend.source_bytes, media_type="image/png")

    @put("/upload")
    async def accept_upload(self, request: Request) -> None:
        backend: MinerTestBackend = request.app.state.backend
        data = await request.body()
        backend.target_path.write_bytes(data)
        log.info(f"  Received upload: {len(data)} bytes -> {backend.target_path}")
        backend.upload_done.set()

    @post("/callback")
    async def accept_callback(self, request: Request, data: AsyncHttpNeuronResponseEnvelope) -> None:
        backend: MinerTestBackend = request.app.state.backend
        backend.callback_envelope = data
        log.info(f"  Received callback: request_id={data.request_id}, error={data.error}")
        backend.callback_done.set()


class MinerTestBackend:
    """State holder and lifecycle manager for the fake miner test server."""

    source_bytes: bytes
    target_path: Path
    upload_done: Event
    callback_done: Event
    callback_envelope: AsyncHttpNeuronResponseEnvelope | None

    base_url: str
    _server: uvicorn.Server | None
    _server_thread: Thread | None

    def __init__(self, *, source_bytes: bytes, target_path: Path):
        self.source_bytes = source_bytes
        self.target_path = target_path
        self.upload_done = Event()
        self.callback_done = Event()
        self.callback_envelope = None
        self.base_url = ""
        self._server = None
        self._server_thread = None

    def start(self) -> None:
        app = Litestar(route_handlers=[MinerTestController], state=State({"backend": self}))
        port = _find_free_port()
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        self._server = uvicorn.Server(config)
        self._server_thread = Thread(target=self._server.run, daemon=True)
        self._server_thread.start()
        _wait_for_port(port)
        self.base_url = f"http://127.0.0.1:{port}"

    def shutdown(self, timeout: float = 5.0) -> None:
        if self._server:
            self._server.should_exit = True
        if self._server_thread:
            self._server_thread.join(timeout=timeout)


@click.command()
@click.argument("source", type=click.Path(exists=True, path_type=Path))
@click.argument("target", type=click.Path(path_type=Path))
@click.option("--miner-url", default="http://127.0.0.1:9090/process", show_default=True)
def main(source: Path, target: Path, miner_url: str) -> None:
    """Test a running cat miner end-to-end."""
    source_bytes = source.read_bytes()
    log.info(f"Source image: {source} ({len(source_bytes)} bytes)")

    backend = MinerTestBackend(source_bytes=source_bytes, target_path=target)
    backend.start()
    log.info(f"Local server listening on {backend.base_url}")

    envelope = AsyncHttpNeuronRequestEnvelope(
        request_id=RequestId(str(uuid.uuid4())),
        callback_url=AnyHttpUrl(f"{backend.base_url}/callback"),
        input=MinerInput(
            input={"image_s3_url": f"{backend.base_url}/source", "image_name": source.name},
            s3_presigned_url=f"{backend.base_url}/upload",
        ).model_dump(),
    )

    log.info(f"Sending task to miner at {miner_url} ...")
    with httpx.Client(timeout=10.0) as client:
        resp = client.post(miner_url, json=envelope.model_dump(mode="json"))
        log.info(f"  Miner responded: {resp.status_code}")
        if resp.status_code != 202:
            log.info(f"  Unexpected status. Body: {resp.text}")
            sys.exit(1)

    log.info("Waiting for miner to process (this may take a while) ...")
    backend.callback_done.wait(timeout=300)

    if not backend.callback_done.is_set():
        log.info("Timed out waiting for callback.")
        sys.exit(1)

    cb = backend.callback_envelope
    if cb and cb.error:
        log.info(f"Miner returned error: {cb.error}")
        sys.exit(1)

    if not backend.upload_done.is_set():
        # Callback arrived but upload didn't — shouldn't happen, but just in case
        log.info("Callback received but no upload. Waiting a bit more ...")
        backend.upload_done.wait(timeout=10)

    backend.shutdown()

    if target.exists():
        log.info(f"Result saved to {target} ({target.stat().st_size} bytes)")
    else:
        log.info("Upload was never received — no output file.")
        sys.exit(1)


if __name__ == "__main__":
    main()
