import logging
from collections.abc import Mapping

from nexus.actors.task_input_output_creator import BatchedTaskInputOutput, TaskInputOutput
from nexus.actors.weight_setter import WeightsCalculationBundle
from nexus.utils.exceptions import NexusTaskName
from nexus.utils.types import Hotkey, Weight

from cat_images.subnet import MinerPayload, MinerPublicResult, MinerResult, ValidationResult

log = logging.getLogger("validation-algorithm")


def validate(
    batch_to_validate: BatchedTaskInputOutput[MinerPayload, MinerResult, MinerPublicResult],
) -> BatchedTaskInputOutput[MinerPayload, ValidationResult, ValidationResult]:
    log.info("Validating batch of miner results!")
    for task_input_output in batch_to_validate.task_input_outputs:
        log.info(
            f"Validating task result {task_input_output.task_result_id} with input {task_input_output.task_input} "
            f"and output {task_input_output.task_output}; public_output={task_input_output.task_public_output}"
        )
    return BatchedTaskInputOutput(
        task_input_outputs=tuple(
            TaskInputOutput(
                task_result_id=task_input_output.task_result_id,
                task_input=task_input_output.task_input,
                task_output=ValidationResult(score=100),
                task_public_output=ValidationResult(score=100),
            )
            for task_input_output in batch_to_validate.task_input_outputs
        )
    )


def weighing_func(
    mining_task_name: NexusTaskName,
    validation_task_name: NexusTaskName,
    task_results_bundle: WeightsCalculationBundle
) -> Mapping[Hotkey, Weight]:
    return {}
