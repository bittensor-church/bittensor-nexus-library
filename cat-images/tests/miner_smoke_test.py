# pyright: basic
"""
Integration test for the cat miner — no real OpenRouter calls.
"""

import base64
import hashlib
import socket
import time
import uuid
from collections.abc import Iterator
from threading import Event, Thread

import httpx
import pytest
import uvicorn
from litestar import Controller, Litestar, Request, Response, get, post, put
from litestar.datastructures import State
from nexus.actors.executor_communicator.async_http_protocol import (
    AsyncHttpNeuronRequestEnvelope,
    AsyncHttpNeuronResponseEnvelope,
    RequestId,
)
from pydantic import AnyHttpUrl

from cat_images.miner import CatMinerSettings, MinerInput, make_miner_service
from cat_images.subnet_models import S3Url, UserImageInput

# Minimal valid 1x1 PNGs
SOURCE_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00"
    b"\x00\x00\x0cIDATx\x9cc\xf8\xcf\xc0\x00\x00\x03\x01"
    b"\x01\x00\xc9\xfe\x92\xef\x00\x00\x00\x00IEND\xaeB`\x82"
)
RESULT_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00"
    b"\x00\x00\x0cIDATx\x9cc``\xf8\x0f\x00\x01\x03\x01"
    b"\x00\x08\x89\xc2\xec\x00\x00\x00\x00IEND\xaeB`\x82"
)


class FakeMinerController(Controller):
    """Litestar controller that fakes source hosting, OpenRouter, S3 upload, and miner callback."""

    path = "/"

    @get("/source")
    async def serve_source(self, request: Request) -> Response:
        backend: FakeMinerBackend = request.app.state.backend
        return Response(content=backend.source_png, media_type="image/png")

    @post("/openrouter")
    async def fake_openrouter(self, request: Request) -> dict:
        backend: FakeMinerBackend = request.app.state.backend
        backend.openrouter_request_body = await request.json()
        return {
            "choices": [
                {"message": {"images": [{"image_url": {"url": f"data:image/png;base64,{backend.result_b64}"}}]}}
            ]
        }

    @put("/upload")
    async def accept_upload(self, request: Request) -> None:
        backend: FakeMinerBackend = request.app.state.backend
        backend.uploaded_bytes = await request.body()
        backend.upload_done.set()

    @post("/callback")
    async def accept_callback(self, request: Request, data: AsyncHttpNeuronResponseEnvelope) -> None:
        backend: FakeMinerBackend = request.app.state.backend
        backend.callback_envelope = data
        backend.callback_done.set()


class FakeMinerBackend:
    """State holder and lifecycle manager for the fake miner backend server."""

    # Test inputs: what the fake server serves
    source_png: bytes
    result_b64: str

    # Captured outputs: what the miner sent to "openrouter"
    openrouter_request_body: dict | None

    # Captured outputs: what the miner sent back to the validator
    uploaded_bytes: bytes
    upload_done: Event
    callback_envelope: AsyncHttpNeuronResponseEnvelope | None
    callback_done: Event

    # Set after start()
    base_url: str
    _server: uvicorn.Server | None
    _server_thread: Thread | None

    def __init__(self, *, source_png: bytes, result_png: bytes):
        self.source_png = source_png
        self.result_b64 = base64.b64encode(result_png).decode()
        self.openrouter_request_body = None
        self.uploaded_bytes = b""
        self.upload_done = Event()
        self.callback_envelope = None
        self.callback_done = Event()
        self.base_url = ""
        self._server = None
        self._server_thread = None

    def start(self) -> None:
        app = Litestar(route_handlers=[FakeMinerController], state=State({"backend": self}))
        port = _find_free_port()
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        self._server = uvicorn.Server(config)
        self._server_thread = Thread(target=self._server.run, daemon=True)
        self._server_thread.start()
        _wait_for_port(port)
        self.base_url = f"http://127.0.0.1:{port}"

    def shutdown(self, timeout: float = 5.0) -> None:
        self._server.should_exit = True
        self._server_thread.join(timeout=timeout)


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
    raise RuntimeError(f"Port {port} not open after {timeout}s")


@pytest.fixture
def fake_backend() -> Iterator[FakeMinerBackend]:
    backend = FakeMinerBackend(source_png=SOURCE_PNG, result_png=RESULT_PNG)
    backend.start()
    yield backend
    backend.shutdown()


def test_miner_end_to_end(fake_backend: FakeMinerBackend) -> None:
    # Test flow:
    # 1. Start miner service pointed at the fake backend (fake OpenRouter, source host, S3, callback)
    # 2. POST an AsyncHttpNeuronRequestEnvelope to the miner (simulating what the validator sends)
    # 3. Miner downloads source image from fake /source
    # 4. Miner sends source image to fake /openrouter, gets back RESULT_PNG
    # 5. Miner uploads RESULT_PNG to fake /upload (simulating S3)
    # 6. Miner POSTs MinerResult (with image hash) to fake /callback
    # 7. We assert: uploaded bytes == RESULT_PNG, callback hash == sha256(RESULT_PNG)
    miner_port = _find_free_port()
    model = "test-model"
    prompt = "add a cat"
    settings = CatMinerSettings(
        openrouter_api_key="fake",
        openrouter_url=f"{fake_backend.base_url}/openrouter",
        openrouter_model=model,
        prompt=prompt,
        port=miner_port,
    )
    service = make_miner_service(settings)
    with service.running():
        _wait_for_port(miner_port)

        # POST request to miner
        envelope = AsyncHttpNeuronRequestEnvelope(
            request_id=RequestId(str(uuid.uuid4())),
            callback_url=AnyHttpUrl(f"{fake_backend.base_url}/callback"),
            input=MinerInput(
                input=UserImageInput(
                    image_s3_url=S3Url(f"{fake_backend.base_url}/source"),
                ),
                presigned_url=f"{fake_backend.base_url}/upload",
            ).model_dump(),
        )
        resp = httpx.post(
            f"http://127.0.0.1:{miner_port}/process",
            json=envelope.model_dump(mode="json"),
            timeout=5.0,
        )
        assert resp.status_code == 202, f"Expected 202, got {resp.status_code}: {resp.text}"

        # Wait for callback
        assert fake_backend.callback_done.wait(timeout=30), "Timed out waiting for miner callback"

    # Assert miner sent the correct OpenRouter request
    source_b64 = base64.b64encode(SOURCE_PNG).decode()
    assert fake_backend.openrouter_request_body == {
        "model": model,
        "modalities": ["image", "text"],
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{source_b64}"}},
                ],
            }
        ],
    }

    # Assert uploaded bytes match expected result
    assert fake_backend.uploaded_bytes == RESULT_PNG

    # Assert callback has correct hash
    cb = fake_backend.callback_envelope
    assert cb is not None
    assert cb.error is None, f"Miner returned error: {cb.error}"
    assert cb.output is not None
    expected_hash = hashlib.sha256(RESULT_PNG).hexdigest()
    assert cb.output["image_hash"] == expected_hash
