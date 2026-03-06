import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from nexus.actors.task_input_output_creator import BatchedTaskInputOutput, TaskInputOutput
from nexus.actors.weight_setter import WeightsCalculationBundle
from nexus.core.runtime.nexus_task_types import NexusTaskName, TaskResultId
from nexus.utils.types import BlockNumber, Epoch, Hotkey

from cat_images.subnet_models import ValidationResult
from cat_images.validator import weighing_algorithm

MINING_TASK_NAME = NexusTaskName("mining-task")
VALIDATION_TASK_NAME = NexusTaskName("validation-task")
CURRENT_EPOCH = Epoch(first_block=BlockNumber(20), last_block=BlockNumber(29))
PREVIOUS_EPOCH = CURRENT_EPOCH.previous()


@dataclass(frozen=True)
class _FakeTarget:
    hotkey: str


@dataclass(frozen=True)
class _FakeBlockAtFinish:
    block_number: BlockNumber


@dataclass(frozen=True)
class _FakeMiningTaskResult:
    id: TaskResultId
    target: _FakeTarget
    is_failure: bool


@dataclass(frozen=True)
class _FakeValidationTaskResult:
    id: TaskResultId
    is_failure: bool
    executor_output: object
    block_at_finish: _FakeBlockAtFinish
    processing_finished: datetime


class _FakeTaskResultStore:
    def __init__(self, by_task_and_epoch: dict[tuple[str, Epoch], tuple[Any, ...]]) -> None:
        self._by_task_and_epoch = by_task_and_epoch

    def get_tasks_for_epoch(self, task_name: NexusTaskName, epoch: Epoch) -> tuple[Any, ...]:
        return self._by_task_and_epoch.get((str(task_name), epoch), ())


def _task_result_id(raw: int) -> TaskResultId:
    return TaskResultId(uuid.UUID(int=raw))


def _mining_result(
    *,
    raw_id: int,
    hotkey: str,
    is_failure: bool,
) -> _FakeMiningTaskResult:
    return _FakeMiningTaskResult(
        id=_task_result_id(raw_id),
        target=_FakeTarget(hotkey=hotkey),
        is_failure=is_failure,
    )


def _validation_batch(
    pairs: tuple[tuple[int, int], ...],
) -> BatchedTaskInputOutput[Any, ValidationResult, ValidationResult]:
    return BatchedTaskInputOutput(
        task_input_outputs=tuple(
            TaskInputOutput(
                task_result_id=_task_result_id(task_result_id_raw),
                task_input={"input": task_result_id_raw},
                task_output=ValidationResult(score=score),
                task_public_output=ValidationResult(score=score),
            )
            for task_result_id_raw, score in pairs
        )
    )


def _validation_result(
    *,
    raw_id: int,
    block_number: int,
    executor_output: object,
    is_failure: bool = False,
    processing_finished_seconds: int = 0,
) -> _FakeValidationTaskResult:
    return _FakeValidationTaskResult(
        id=_task_result_id(raw_id),
        is_failure=is_failure,
        executor_output=executor_output,
        block_at_finish=_FakeBlockAtFinish(block_number=BlockNumber(block_number)),
        processing_finished=datetime(2026, 3, 5, 0, 0, tzinfo=UTC) + timedelta(seconds=processing_finished_seconds),
    )


def _run_weighing(
    *,
    mining_previous: tuple[_FakeMiningTaskResult, ...],
    mining_current: tuple[_FakeMiningTaskResult, ...] = (),
    validation_previous: tuple[_FakeValidationTaskResult, ...] = (),
    validation_current: tuple[_FakeValidationTaskResult, ...] = (),
) -> dict[Hotkey, float]:
    store = _FakeTaskResultStore(
        by_task_and_epoch={
            (str(MINING_TASK_NAME), PREVIOUS_EPOCH): mining_previous,
            (str(MINING_TASK_NAME), CURRENT_EPOCH): mining_current,
            (str(VALIDATION_TASK_NAME), PREVIOUS_EPOCH): validation_previous,
            (str(VALIDATION_TASK_NAME), CURRENT_EPOCH): validation_current,
        }
    )
    bundle = WeightsCalculationBundle(
        epoch=CURRENT_EPOCH,
        tasks_result_store=cast(Any, store),
    )

    weights = weighing_algorithm.weighing_func(
        MINING_TASK_NAME,
        VALIDATION_TASK_NAME,
        bundle,
    )
    return {hotkey: float(weight) for hotkey, weight in weights.items()}


def test_weighing_uses_only_previous_epoch_mining_results() -> None:
    weights = _run_weighing(
        mining_previous=(
            _mining_result(raw_id=1, hotkey="hk1", is_failure=False),
        ),
        mining_current=(
            _mining_result(raw_id=2, hotkey="hk1", is_failure=False),
        ),
        validation_current=(
            _validation_result(
                raw_id=101,
                block_number=25,
                executor_output=_validation_batch(((1, 80), (2, 10))),
            ),
        ),
    )

    assert weights == {Hotkey("hk1"): pytest.approx(80.0)}


def test_weighing_uses_validation_results_from_previous_and_current_epoch() -> None:
    weights = _run_weighing(
        mining_previous=(
            _mining_result(raw_id=1, hotkey="hk1", is_failure=False),
            _mining_result(raw_id=2, hotkey="hk1", is_failure=False),
        ),
        validation_previous=(
            _validation_result(raw_id=201, block_number=18, executor_output=_validation_batch(((1, 60),))),
        ),
        validation_current=(
            _validation_result(raw_id=202, block_number=24, executor_output=_validation_batch(((2, 90),))),
        ),
    )

    assert weights == {Hotkey("hk1"): pytest.approx(150.0)}


def test_weighing_counts_failed_mining_task_as_zero_score() -> None:
    weights = _run_weighing(
        mining_previous=(
            _mining_result(raw_id=1, hotkey="hk1", is_failure=True),
            _mining_result(raw_id=2, hotkey="hk1", is_failure=False),
        ),
        validation_current=(
            _validation_result(raw_id=301, block_number=23, executor_output=_validation_batch(((2, 100),))),
        ),
    )

    assert weights == {Hotkey("hk1"): pytest.approx(100.0)}


def test_weighing_excludes_success_without_validation_from_average() -> None:
    weights = _run_weighing(
        mining_previous=(
            _mining_result(raw_id=1, hotkey="hk1", is_failure=False),
            _mining_result(raw_id=2, hotkey="hk1", is_failure=False),
        ),
        validation_current=(
            _validation_result(raw_id=401, block_number=22, executor_output=_validation_batch(((1, 80),))),
        ),
    )

    assert weights == {Hotkey("hk1"): pytest.approx(160.0)}
