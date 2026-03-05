import base64
import hashlib
import logging
import sys
import time
from contextlib import nullcontext
from datetime import timedelta
from typing import Any, Self

import httpx
from nexus.actors.executor_communicator import AsyncHttpNeuronService
from nexus.actors.payload_creator import WithPresignedUrl
from nexus.utils.types import NetUid, Port
from pydantic import ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .axon_updater import AxonUpdaterConfig, AxonUpdaterService
from .subnet import ImageHash, MinerResult, SingleCatImageInput

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s", datefmt="%H:%M:%S", level=logging.INFO
)
log = logging.getLogger("miner")

DEFAULT_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_OPENROUTER_MODEL = "google/gemini-2.5-flash-image"
DEFAULT_PROMPT = (
    "Edit this image to add a cat fitting naturally into the scene. Consider what the input image represents"
    " and how a cat could be naturally placed there. Keep the original image intact."
)
DEFAULT_PORT = Port(9090)
DEFAULT_PATH = "/process"

_RETRY_TRANSPORT = httpx.HTTPTransport(retries=3)


class CatMinerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MINER_", env_file=".env")

    openrouter_api_key: str
    openrouter_url: str = DEFAULT_OPENROUTER_URL
    openrouter_model: str = DEFAULT_OPENROUTER_MODEL
    prompt: str = DEFAULT_PROMPT
    port: Port = DEFAULT_PORT
    path: str = DEFAULT_PATH

    # Axon updater — periodically registers serving address on chain
    update_axon: bool = False
    wallet_name: str = "default"
    hotkey_name: str = "default"
    subtensor_network: str = "finney"
    netuid: NetUid | None = None
    external_ip: str | None = None
    external_port: Port | None = None
    serve_interval: timedelta = timedelta(seconds=60)

    @model_validator(mode="after")
    def _axon_updater_requires_netuid(self) -> Self:
        if self.update_axon and self.netuid is None:
            raise ValueError("MINER_NETUID is required when MINER_UPDATE_AXON is enabled")
        return self


MinerInput = WithPresignedUrl[SingleCatImageInput]


def _download_image(url: str) -> bytes:
    log.info(f"Downloading source image from {url}")
    with httpx.Client(transport=_RETRY_TRANSPORT) as client:
        resp = client.get(url)
        resp.raise_for_status()
        log.info(f"Downloaded {len(resp.content)} bytes")
        return resp.content


def _add_cat_to_image(image_bytes: bytes, *, settings: CatMinerSettings) -> bytes:
    """Call OpenRouter to edit the image, adding a cat."""
    b64_image = base64.b64encode(image_bytes).decode()
    data_url = f"data:image/png;base64,{b64_image}"

    payload: dict[str, Any] = {
        "model": settings.openrouter_model,
        "modalities": ["image", "text"],
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": settings.prompt},
                    {"type": "image_url", "image_url": {"url": data_url}},
                ],
            }
        ],
    }

    log.info(f"Sending image to OpenRouter model={settings.openrouter_model}")
    with httpx.Client(transport=_RETRY_TRANSPORT, timeout=120.0) as client:
        resp = client.post(
            settings.openrouter_url,
            json=payload,
            headers={"Authorization": f"Bearer {settings.openrouter_api_key}"},
        )
        resp.raise_for_status()

    # OpenRouter returns images in choices[0].message.images[0].image_url.url
    # as a data URL: "data:image/png;base64,..."
    result = resp.json()
    image_data_url: str = result["choices"][0]["message"]["images"][0]["image_url"]["url"]
    b64_data = image_data_url.split(",", maxsplit=1)[1]
    output = base64.b64decode(b64_data)
    log.info(f"Received {len(output)} bytes from OpenRouter")
    return output


def _sha256(data: bytes) -> ImageHash:
    return ImageHash(hashlib.sha256(data).hexdigest())


def make_processor(settings: CatMinerSettings):
    def process(task: MinerInput) -> MinerResult:
        log.info(f"Processing task: image_name={task.input.image_name}")
        source_bytes = _download_image(str(task.input.image_s3_url))
        cat_bytes = _add_cat_to_image(source_bytes, settings=settings)
        log.info(f"Uploading result to {task.presigned_url}")
        with httpx.Client(transport=_RETRY_TRANSPORT) as client:
            client.put(str(task.presigned_url), content=cat_bytes).raise_for_status()
        image_hash = _sha256(cat_bytes)
        log.info(f"Done: hash={image_hash}")
        return MinerResult(image_hash=image_hash)

    return process


def _load_settings() -> CatMinerSettings:
    try:
        return CatMinerSettings()  # type: ignore[call-arg]
    except ValidationError as e:
        fields = ", ".join(str(err["loc"][-1]) for err in e.errors() if err.get("loc"))
        log.error(f"Configuration error: missing or invalid fields: {fields}")
        log.error("Check your .env file or environment variables.")
        sys.exit(1)


def make_miner_service(
    settings: CatMinerSettings | None = None,
) -> AsyncHttpNeuronService[MinerInput, MinerResult]:
    if settings is None:
        settings = _load_settings()
    return AsyncHttpNeuronService(
        path=settings.path,
        port=settings.port,
        input_model=MinerInput,
        output_model=MinerResult,
        processor=make_processor(settings),
    )


def make_axon_updater_service(settings: CatMinerSettings) -> AxonUpdaterService | None:
    if not settings.update_axon:
        log.warning("Axon updates are disabled - will not set axon address on chain")
        return None
    if settings.netuid is None:
        raise RuntimeError("netuid is required when update_axon is enabled")
    return AxonUpdaterService(
        AxonUpdaterConfig(
            wallet_name=settings.wallet_name,
            hotkey_name=settings.hotkey_name,
            subtensor_network=settings.subtensor_network,
            netuid=settings.netuid,
            port=settings.port,
            external_ip=settings.external_ip,
            external_port=settings.external_port,
            interval=settings.serve_interval,
        )
    )


def main() -> None:
    settings = _load_settings()
    service = make_miner_service(settings)
    updater = make_axon_updater_service(settings)

    with service.running(), updater.running() if updater else nullcontext():
        log.info(f"Cat miner listening on port {service.bound_port}. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
