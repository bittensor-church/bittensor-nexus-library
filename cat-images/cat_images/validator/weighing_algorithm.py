# pyright: basic

import logging
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

from nexus.actors.openrouter_selection import Fields, ScalarField
from nexus.actors.weight_setter import WeightsCalculationBundle
from nexus.core.runtime.nexus_task_types import NexusTaskName
from nexus.utils.exceptions import InternalFrameworkException
from nexus.utils.types import Hotkey, Weight

log = logging.getLogger("weighing-algorithm")


@dataclass
class _HotkeyAccumulator:
    mining_count: int = 0
    scored_count: int = 0
    scored_sum: float = 0.0
    unscored_success_count: int = 0


class _ValidationExecutorPayloadLike(Protocol):
    """Validation payload shape exposing normalized OpenRouter request fields."""

    @property
    def fields(self) -> Sequence[Fields]: ...


def _requested_task_result_id(selected_item: Fields) -> str:
    task_result_id = selected_item.fields.get("task_result_id")
    if isinstance(task_result_id, ScalarField) and isinstance(task_result_id.value, str):
        return task_result_id.value

    raise InternalFrameworkException("Validation payload item must include task_result_id as ScalarField[str].")


def _requested_task_result_ids(validation_payload: object) -> set[str]:
    fields = cast(_ValidationExecutorPayloadLike, validation_payload).fields
    requested_ids: set[str] = set()
    for selected_item in fields:
        requested_ids.add(_requested_task_result_id(selected_item))
    return requested_ids


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

    mining_successes = task_results.get_successful_tasks_for_epoch(task_name=mining_task_name, epoch=previous_epoch)
    mining_failures = task_results.get_executor_failures_for_epoch(task_name=mining_task_name, epoch=previous_epoch)
    validation_successes = task_results.get_successful_tasks_for_epoch(
        task_name=validation_task_name, epoch=previous_epoch
    ) + task_results.get_successful_tasks_for_epoch(task_name=validation_task_name, epoch=current_epoch)

    validation_scores_by_mining_task_result_id: dict[str, int] = {}
    for validation_result in validation_successes:
        try:
            requested_task_result_ids = _requested_task_result_ids(validation_result.executor_payload)
        except InternalFrameworkException as exc:
            log.warning(
                "Ignoring validation result %s due to malformed OpenRouter request payload: %s",
                validation_result.id,
                exc,
            )
            continue
        validation_output: TaskScores = validation_result.executor_output
        returned_scores = validation_output.scores_by_task_result_id

        if len(requested_task_result_ids) == 1 and set(returned_scores.keys()) != requested_task_result_ids:
            log.warning(
                "Ignoring singleton validation result %s due to requested_ids=%s returned_ids=%s",
                validation_result.id,
                sorted(requested_task_result_ids),
                sorted(returned_scores.keys()),
            )
            continue

        for task_result_id in requested_task_result_ids:
            score = returned_scores.get(task_result_id)
            if score is not None:
                validation_scores_by_mining_task_result_id[task_result_id] = score

    accumulators_by_hotkey: dict[Hotkey, _HotkeyAccumulator] = defaultdict(_HotkeyAccumulator)

    for mining_task_result in mining_successes:
        hotkey = Hotkey(mining_task_result.target.hotkey)
        accumulator = accumulators_by_hotkey[hotkey]
        accumulator.mining_count += 1

        score = validation_scores_by_mining_task_result_id.get(str(mining_task_result.id))
        if score is None:
            accumulator.unscored_success_count += 1
            continue

        accumulator.scored_count += 1
        accumulator.scored_sum += float(score)

    for mining_failure in mining_failures:
        hotkey = Hotkey(mining_failure.target.hotkey)
        accumulator = accumulators_by_hotkey[hotkey]
        accumulator.mining_count += 1
        accumulator.scored_count += 1

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
