# pyright: basic

import logging
from datetime import timedelta
from functools import partial
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
from nexus.actors.payload_creator import NoopPayloadCreator, PresignedUrlCreator
from nexus.actors.retry_strategy import RetryStrategy
from nexus.actors.task_input_output_creator import BatchedTaskInputOutput, TaskInputOutputCreator
from nexus.actors.task_result_sampler import EveryTaskResultSampler
from nexus.actors.weight_setter import WeightSetterNode
from nexus.core.runtime.nexus_task import NexusTask
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.nexus_validator import NexusValidator
from nexus.utils.types import BlockCount

from cat_images.subnet_models import (
    MinerPayload,
    MinerPublicResult,
    MinerResult,
    SingleCatImageInput,
    ValidationResult,
)

from . import validation_algorithm, weighing_algorithm
from .validator_settings import CatValidatorSettings

MINING_TASK_NAME = NexusTaskName("add-cat-to-image")
VALIDATION_TASK_NAME = NexusTaskName("validation-task")

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-7s %(message)s", datefmt="%H:%M:%S", level=logging.INFO
)
log = logging.getLogger("validator")


class Validator(NexusValidator):
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
                executor_func=partial(
                    validation_algorithm.validate,
                    settings=settings,
                ),
            ),
            executor_result_converter=NoopPayloadCreator("validation-result-converter"),
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
