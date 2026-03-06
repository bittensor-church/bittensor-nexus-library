# pyright: basic

import logging
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from nexus.actors.task_input_output_creator import BatchedTaskInputOutput
from nexus.actors.weight_setter import WeightsCalculationBundle
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.utils.types import Hotkey, Weight

from cat_images.subnet_models import MinerPayload, ValidationResult

log = logging.getLogger("weighing-algorithm")


@dataclass
class _HotkeyAccumulator:
    mining_count: int = 0
    scored_count: int = 0
    scored_sum: float = 0.0
    unscored_success_count: int = 0


def weighing_func(
    mining_task_name: NexusTaskName,
    validation_task_name: NexusTaskName,
    task_results_bundle: WeightsCalculationBundle,
) -> Mapping[Hotkey, Weight]:
    current_epoch = task_results_bundle.epoch
    previous_epoch = current_epoch.previous()
    task_results = task_results_bundle.tasks_result_store

    log.info(
        "Calculating weights with mining_epoch=%s and validation_epochs=[%s, %s]",
        previous_epoch,
        previous_epoch,
        current_epoch,
    )

    mining_task_results = task_results.get_tasks_for_epoch(task_name=mining_task_name, epoch=previous_epoch)
    validation_task_results = (
        task_results.get_tasks_for_epoch(task_name=validation_task_name, epoch=previous_epoch)
        + task_results.get_tasks_for_epoch(task_name=validation_task_name, epoch=current_epoch)
    )

    validation_scores_by_mining_task_result_id: dict[str, int] = {}
    for validation_result in validation_task_results:
        if validation_result.is_failure:
            continue
        validation_output = cast(
            BatchedTaskInputOutput[MinerPayload, ValidationResult, ValidationResult],
            validation_result.executor_output,
        )
        for task_input_output in validation_output.task_input_outputs:
            validation_scores_by_mining_task_result_id[str(task_input_output.task_result_id)] = (
                task_input_output.task_output.score
            )

    accumulators_by_hotkey: dict[Hotkey, _HotkeyAccumulator] = defaultdict(_HotkeyAccumulator)

    for mining_task_result in mining_task_results:
        hotkey = Hotkey(mining_task_result.target.hotkey)
        accumulator = accumulators_by_hotkey[hotkey]
        accumulator.mining_count += 1

        if mining_task_result.is_failure:
            accumulator.scored_count += 1
            continue

        score = validation_scores_by_mining_task_result_id.get(str(mining_task_result.id))
        if score is None:
            accumulator.unscored_success_count += 1
            continue

        accumulator.scored_count += 1
        accumulator.scored_sum += float(score)

    weights_by_hotkey: dict[Hotkey, Weight] = {}
    for hotkey in sorted(accumulators_by_hotkey.keys(), key=str):
        accumulator = accumulators_by_hotkey[hotkey]
        average_score = accumulator.scored_sum / accumulator.scored_count if accumulator.scored_count > 0 else 0.0
        total_score = float(accumulator.mining_count) * average_score
        weights_by_hotkey[hotkey] = Weight(total_score)
        log.info(
            "hotkey=%s mining_tasks=%s scored_tasks=%s unscored_successes=%s avg_score=%.2f total_score=%.2f",
            hotkey,
            accumulator.mining_count,
            accumulator.scored_count,
            accumulator.unscored_success_count,
            average_score,
            total_score,
        )

    if len(weights_by_hotkey) == 0:
        log.info("Computed weights by hotkey: <empty>")
    else:
        weight_lines = [
            f"hotkey={hotkey} weight={float(weights_by_hotkey[hotkey]):.4f}"
            for hotkey in sorted(weights_by_hotkey.keys(), key=str)
        ]
        log.info("Computed weights by hotkey:\n%s", "\n".join(weight_lines))

    return weights_by_hotkey
