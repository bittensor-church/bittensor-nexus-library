# pyright: basic

import logging
import sys
import time
from datetime import timedelta
from ipaddress import IPv4Address

from nexus.actors import (
    AsyncHttpNeuronCommunicator,
    EpochBeatNode,
    RestEntryPoint,
    RoundRobinNeuronRouter,
    miners_only,
)
from nexus.actors.executor_communicator.embedded_executor_communicator import EmbeddedExecutorCommunicator
from nexus.actors.neuron_router import NoopRouter
from nexus.actors.payload_creator import NoopPayloadCreator, PresignedUrlCreator, WithPresignedUrl
from nexus.actors.retry_strategy import RetryStrategy
from nexus.actors.task_input_output_creator import BatchedTaskInputOutput, TaskInputOutputCreator
from nexus.actors.task_result_sampler import EveryTaskResultSampler, TaskResultSampler
from nexus.actors.weight_setter import WeightSetterNode
from nexus.core.runtime.nexus_task import NexusTask, SingleTaskResult
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.subnet_runtime import SubnetRuntime
from nexus.nexus_validator import NexusValidator
from nexus.utils.types import BlockCount, NetUid, Port
from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from cat_images import validation_algorithm

from .subnet import (
    MinerPayload,
    MinerPayloadModel,
    MinerPublicResult,
    MinerResult,
    SingleCatImageInput,
    ValidationResult,
)

MINING_TASK_NAME = NexusTaskName("add-cat-to-image")
VALIDATION_TASK_NAME = NexusTaskName("validation-task")
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


class Validator(NexusValidator):
    # these annotations are optional but help with readability and IDE support
    # they are also a perfect source of knowledge for an LLM
    entry: RestEntryPoint[SingleCatImageInput]

    mining_task: NexusTask[SingleCatImageInput, MinerPayload, MinerResult, WithPresignedUrl[MinerResult]]
    miner_result_sampler: TaskResultSampler[MinerPayload, MinerResult, WithPresignedUrl[MinerResult]]
    validation_task: NexusTask[
        tuple[SingleTaskResult[MinerPayload, MinerResult, MinerPublicResult], ...],
        BatchedTaskInputOutput[MinerPayload, MinerResult, MinerPublicResult],
        BatchedTaskInputOutput[MinerPayload, ValidationResult, ValidationResult],
    ]

    epoch_beat: EpochBeatNode
    weight_setter: WeightSetterNode

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
            payload_creator=PresignedUrlCreator("miner-upload-url", bucket=settings.s3_bucket, method="PUT"),
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
            executor_result_converter=PresignedUrlCreator(
                "create-get-url-for-miner-image",
                method="GET",
                load_s3_key="miner-upload-url",
                bucket=settings.s3_bucket),
            task_result_store_provider=self.task_result_store_provider,  # this should go once we set up dependency injection
        )

        self.miner_result_sampler = EveryTaskResultSampler("miner-result-sampler")

        self.validation_task = NexusTask(
            name=VALIDATION_TASK_NAME,
            retry=RetryStrategy("validation-task-retry", max_attempts=1, delay=timedelta(seconds=1.0)),
            payload_creator=TaskInputOutputCreator("create-payload-for-validation-task"),
            router=NoopRouter("validation-router"),
            executor_communicator=EmbeddedExecutorCommunicator(
                "validator-communicator",
                input_model=BatchedTaskInputOutput[MinerPayload, MinerResult, MinerPublicResult],
                output_model=BatchedTaskInputOutput[MinerPayload, ValidationResult, ValidationResult],
                executor_func=validation_algorithm.validate,
            ),
            executor_result_converter=NoopPayloadCreator("validation-result-converter"),
            task_result_store_provider=self.task_result_store_provider,  # this should go once we set up dependency injection
        )

        self.epoch_beat = EpochBeatNode(
            "weight-setting-trigger",
            netuid=settings.netuid,
            delay=BlockCount(20),
            pylon_client_provider=self.pylon_client_provider,  # this should go once we set up dependency injection
        )

        self.weight_setter = WeightSetterNode(
            "cat-images-weight-setter",
            pylon_client_provider=self.pylon_client_provider,  # this should go once we set up dependency injection
            task_result_store_provider=self.task_result_store_provider,  # this should go once we set up dependency injection
            weighing_func=lambda task_results_bundle: validation_algorithm.weighing_func(
                MINING_TASK_NAME, VALIDATION_TASK_NAME, task_results_bundle
            ),
        )

        self.add_nodes(
            self.entry,
            self.mining_task,
            self.miner_result_sampler,
            self.validation_task,
            self.epoch_beat,
            self.weight_setter,
        )

        # mining
        self.connect(self.entry.source, self.mining_task.input)
        self.connect(self.mining_task.executor_output, self.entry.sink)
        self.connect(self.mining_task.error, self.entry.sink)

        # validation
        self.connect(self.mining_task.task_result, self.miner_result_sampler.task_results)
        self.connect(self.miner_result_sampler.sampled_batch, self.validation_task.input)

        # weight setting
        self.connect(self.epoch_beat.source, self.weight_setter.sink)


def _load_settings() -> CatValidatorSettings:
    try:
        return CatValidatorSettings()  # type: ignore[call-arg]
    except ValidationError as e:
        fields = ", ".join(str(err["loc"][-1]) for err in e.errors() if err.get("loc"))
        log.error(f"Configuration error: missing or invalid fields: {fields}")
        log.error("Check your .env file or environment variables.")
        sys.exit(1)


def main() -> None:
    logging.getLogger("httpx").setLevel(logging.WARN)
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
