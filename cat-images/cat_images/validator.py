# pyright: basic

import logging
import sys
import time
from collections.abc import Sequence
from datetime import timedelta
from ipaddress import IPv4Address

from nexus.actors import (
    AsyncHttpNeuronCommunicator,
    RestEntryPoint,
    RoundRobinNeuronRouter,
    miners_only,
)
from nexus.actors.payload_creator import S3PresignedUrlCreator
from nexus.actors.retry_strategy import RetryStrategy
from nexus.core.runtime.nexus_task import NexusTask
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.subnet_runtime import SubnetRuntime
from nexus.nexus_validator import NexusValidator
from nexus.utils.types import NetUid, Port
from pylon_client.artanis.v1 import Neuron
from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from .subnet import MinerPayload, MinerPayloadModel, MinerResult, SingleCatImageInput

MINING_TASK_NAME = NexusTaskName("add-cat-to-image")
NETUID = NetUid(1)

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s", datefmt="%H:%M:%S", level=logging.INFO
)
log = logging.getLogger("validator")

DEFAULT_OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_OPENROUTER_MODEL = "google/gemini-2.5-flash-image"
DEFAULT_S3_BUCKET = "my-cat-images-bucket"
DEFAULT_INGRESS_PORT = Port(8081)
DEFAULT_MINER_CALLBACK_PORT = Port(9091)


class CatValidatorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VALIDATOR_", env_file=".env")

    rest_entry_point_port: Port = DEFAULT_INGRESS_PORT
    miner_callback_port: Port = DEFAULT_MINER_CALLBACK_PORT

    openrouter_url: str = DEFAULT_OPENROUTER_URL
    openrouter_model: str = DEFAULT_OPENROUTER_MODEL

    netuid: NetUid
    openrouter_api_key: str
    external_ip: str
    pylon_service_address: str
    pylon_open_access_token: str
    s3_bucket: str = DEFAULT_S3_BUCKET


# def _make_miner_filter(target_hotkey: str | None):
#     def _filter(neurons: Sequence[Neuron]) -> Sequence[Neuron]:
#         filtered = miners_only(neurons)
#         if target_hotkey is None:
#             return filtered
#         return [neuron for neuron in filtered if neuron.hotkey == target_hotkey]
#
#     return _filter


class Validator(NexusValidator):
    # these annotations are optional but help with readability and IDE support
    # they are also a perfect source of knowledge for an LLM
    entry: RestEntryPoint[SingleCatImageInput]

    mining_task: NexusTask[SingleCatImageInput, MinerResult, MinerPayload]
    # miner_result_sampler: TaskResultSampler[MinerPayload, MinerResult]

    runtime: SubnetRuntime

    def __init__(self, settings: CatValidatorSettings) -> None:
        super().__init__(settings)

        self.entry = RestEntryPoint(
            _id="cat-images-user-requests",
            path="/cat-images",
            port=settings.rest_entry_point_port,
            user_data_model=SingleCatImageInput,
        )

        self.mining_task = NexusTask(
            name=MINING_TASK_NAME,
            retry=RetryStrategy("mining-task-retry", max_attempts=6, delay=timedelta(seconds=1.0)),
            payload_creator=S3PresignedUrlCreator("create-payload-for-mining-task", bucket=settings.s3_bucket),
            router=RoundRobinNeuronRouter(
                "mining-router",
                netuid=settings.netuid,
                neuron_filter=miners_only,
                pylon_client_provider=self.pylon_client_provider,  # this should go once we set up dependency injection
            ),
            executor_communicator=AsyncHttpNeuronCommunicator(
                "miner-communicator",
                target_path="/process",
                callback_bind_ip=IPv4Address("0.0.0.0"),
                callback_port=settings.miner_callback_port,
                callback_path="/mined-image",
                callback_base_url=f"http://{settings.external_ip}:{settings.miner_callback_port}",
                send_timeout=timedelta(seconds=1),
                total_processing_timeout=timedelta(seconds=60),
                input_model=MinerPayloadModel,
                output_model=MinerResult,
            ),
        )

        self.add_nodes(self.entry, self.mining_task)

        self.connect(self.entry.source, self.mining_task.input)
        self.connect(self.mining_task.executor_output, self.entry.sink)
        self.connect(self.mining_task.error, self.entry.sink)


def _load_settings() -> CatValidatorSettings:
    try:
        return CatValidatorSettings()  # type: ignore[call-arg]
    except ValidationError as e:
        fields = ", ".join(str(err["loc"][-1]) for err in e.errors() if err.get("loc"))
        log.error(f"Configuration error: missing or invalid fields: {fields}")
        log.error("Check your .env file or environment variables.")
        sys.exit(1)


def main() -> None:
    settings = _load_settings()

    validator = Validator(settings)
    with validator.start_runtime():
        print("Validator running. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
