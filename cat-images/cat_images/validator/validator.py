# pyright: basic

import logging
from datetime import timedelta

from nexus.actors import (
    AsyncHttpNeuronCommunicator,
    EpochBeatNode,
    MultiOpenRouterPayloadCreator,
    OpenRouterInferenceCommunicator,
    OpenRouterInferenceRequest,
    RestEntryPoint,
    RoundRobinNeuronRouter,
    miners_only,
)
from nexus.actors.neuron_router import NoopRouter
from nexus.actors.openrouter_selection import ImageUrlField, ScalarField
from nexus.actors.payload_creator import NoopPayloadCreator, PresignedUrlCreator
from nexus.actors.retry_strategy import RetryStrategy
from nexus.actors.task_result_sampler import EveryTaskResultSampler
from nexus.actors.weight_setter import WeightSetterNode
from nexus.core.runtime.nexus_task import NexusTask
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.core.runtime.task_result_store import SuccessfulTaskResult
from nexus.nexus_validator import NexusValidator
from nexus.utils.types import BlockCount

from cat_images.subnet_models import (
    MinerPayload,
    MinerPublicResult,
    MinerResult,
    TaskScores,
    UserImageInput,
)

from . import weighing_algorithm
from .validator_settings import CatValidatorSettings

MINING_TASK_NAME = NexusTaskName("add-cat-to-image")
VALIDATION_TASK_NAME = NexusTaskName("validation-task")

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s", datefmt="%H:%M:%S", level=logging.INFO
)
log = logging.getLogger("validator")


class Validator(NexusValidator):
    """Validator graph for the cat-images subnet, including mining, validation, and weight setting."""

    def __init__(self, settings: CatValidatorSettings) -> None:
        super().__init__(settings)

        self.entry = RestEntryPoint(
            _id="cat-images-user-requests",
            path="/cat-images",
            port=settings.rest_entry_point_port,
            user_data_model=UserImageInput,
        )

        self.mining_task = NexusTask(
            name=MINING_TASK_NAME,
            retry=RetryStrategy("mining-task-retry", max_attempts=6, delay=timedelta(seconds=1.0)),
            payload_creator=PresignedUrlCreator("miner-upload-url", bucket=settings.s3_bucket, method="PUT"),
            router=RoundRobinNeuronRouter(
                "mining-router",
                netuid=settings.netuid,
                neuron_filter=miners_only,
            ),
            executor_communicator=AsyncHttpNeuronCommunicator(
                "miner-communicator",
                target_path="/process",
                callback_port=settings.miner_callback_port,
                callback_path="/mined-image",
                callback_base_url=f"http://{settings.external_ip}:{settings.miner_callback_port}",
                send_timeout=timedelta(seconds=1),
                total_processing_timeout=timedelta(seconds=60),
                input_model=MinerPayload,
                output_model=MinerResult,
            ),
            executor_result_converter=PresignedUrlCreator(
                "create-get-url-for-miner-image",
                method="GET",
                load_s3_key="miner-upload-url",
                bucket=settings.s3_bucket,
            ),
        )

        self.miner_result_sampler = EveryTaskResultSampler[MinerPayload, MinerResult, MinerPublicResult](
            "miner-result-sampler"
        )

        self.validation_task = NexusTask[
            tuple[SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult], ...],
            OpenRouterInferenceRequest,
            TaskScores,
        ](
            name=VALIDATION_TASK_NAME,
            retry=RetryStrategy("validation-task-retry", max_attempts=1, delay=timedelta(seconds=1.0)),
            payload_creator=MultiOpenRouterPayloadCreator[
                SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult]
            ](
                "create-payload-for-validation-task",
                user_prompt=settings.validation_prompt,
                item_selector=lambda task_result: {
                    "original_image_url": ImageUrlField(url=str(task_result.executor_payload.input.image_s3_url)),
                    "generated_image_url": ImageUrlField(url=str(task_result.executor_public_output.presigned_url)),
                    "task_result_id": ScalarField(value=str(task_result.id)),
                },
            ),
            router=NoopRouter[OpenRouterInferenceRequest]("validation-router"),
            executor_communicator=OpenRouterInferenceCommunicator[TaskScores](
                "validator-communicator",
                output_model=TaskScores,
            ),
            executor_result_converter=NoopPayloadCreator[TaskScores]("validation-result-converter"),
        )

        self.epoch_beat = EpochBeatNode(
            "weight-setting-trigger",
            netuid=settings.netuid,
            delay=BlockCount(20),
        )

        self.weight_setter = WeightSetterNode(
            "cat-images-weight-setter",
            weighing_func=lambda task_results_bundle: weighing_algorithm.weighing_func(
                MINING_TASK_NAME, VALIDATION_TASK_NAME, task_results_bundle
            ),
        )

        # mining
        self.connect(self.entry.source, self.mining_task.input)
        self.connect(self.mining_task.executor_output, self.entry.sink)
        self.connect(self.mining_task.error, self.entry.sink)

        # validation
        self.connect(self.mining_task.successful_task_result, self.miner_result_sampler.task_results)
        self.connect(self.miner_result_sampler.sampled_batch, self.validation_task.input)

        # weight setting
        self.connect(self.epoch_beat.source, self.weight_setter.sink)
