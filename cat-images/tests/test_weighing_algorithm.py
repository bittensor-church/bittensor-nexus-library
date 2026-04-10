import uuid
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

from nexus.actors.openrouter_selection import Fields, ScalarField
from nexus.actors.weight_setter import WeightsCalculationBundle
from nexus.core.runtime.nexus_task_types import NexusTaskName, TaskResultId
from nexus.utils.types import BlockNumber, Epoch, Hotkey

from cat_images.validator import weighing_algorithm
from cat_images.validator.openrouter_inference import TaskScores

MINING_TASK_NAME = NexusTaskName("mining-task")
VALIDATION_TASK_NAME = NexusTaskName("validation-task")
CURRENT_EPOCH = Epoch(first_block=BlockNumber(20), last_block=BlockNumber(29))
PREVIOUS_EPOCH = CURRENT_EPOCH.previous()


@dataclass(frozen=True)
class _FakeTarget:
    """Minimal routing target stub that carries the miner hotkey."""

    hotkey: str


@dataclass(frozen=True)
class _FakeSuccessfulMiningTaskResult:
    """Successful mining task result used by weighing tests."""

    id: TaskResultId
    target: _FakeTarget


@dataclass(frozen=True)
class _FakeExecutorFailureTaskResult:
    """Executor failure task result used by weighing tests."""

    id: TaskResultId
    target: _FakeTarget


@dataclass(frozen=True)
class _FakeSuccessfulValidationTaskResult:
    """Successful validation task result carrying the OpenRouter request and scores."""

    id: TaskResultId
    executor_payload: object
    executor_output: object


@dataclass(frozen=True)
class _FakeValidationExecutorPayload:
    """Validation payload stub exposing normalized OpenRouter fields to the weighing algorithm."""

    fields: tuple[Fields, ...]


class _FakeTaskResultStore:
    """Store stub exposing only the explicit success and executor-failure epoch queries."""

    def __init__(
        self,
        *,
        successful_by_task_and_epoch: dict[tuple[str, Epoch], tuple[Any, ...]],
        executor_failures_by_task_and_epoch: dict[tuple[str, Epoch], tuple[Any, ...]],
    ) -> None:
        self._successful_by_task_and_epoch = successful_by_task_and_epoch
        self._executor_failures_by_task_and_epoch = executor_failures_by_task_and_epoch
        self.calls: list[tuple[str, str, Epoch]] = []

    def get_successful_tasks_for_epoch(self, task_name: NexusTaskName, epoch: Epoch) -> tuple[Any, ...]:
        self.calls.append(("success", str(task_name), epoch))
        return self._successful_by_task_and_epoch.get((str(task_name), epoch), ())

    def get_executor_failures_for_epoch(self, task_name: NexusTaskName, epoch: Epoch) -> tuple[Any, ...]:
        self.calls.append(("executor_failure", str(task_name), epoch))
        return self._executor_failures_by_task_and_epoch.get((str(task_name), epoch), ())


def _task_result_id(raw: int) -> TaskResultId:
    return TaskResultId(uuid.UUID(int=raw))


def _mining_success(*, raw_id: int, hotkey: str) -> _FakeSuccessfulMiningTaskResult:
    return _FakeSuccessfulMiningTaskResult(
        id=_task_result_id(raw_id),
        target=_FakeTarget(hotkey=hotkey),
    )


def _mining_executor_failure(*, raw_id: int, hotkey: str) -> _FakeExecutorFailureTaskResult:
    return _FakeExecutorFailureTaskResult(
        id=_task_result_id(raw_id),
        target=_FakeTarget(hotkey=hotkey),
    )


def _validation_batch(
    pairs: tuple[tuple[int, int], ...],
) -> TaskScores:
    return TaskScores(
        scores_by_task_result_id={
            str(_task_result_id(task_result_id_raw)): score for task_result_id_raw, score in pairs
        }
    )


def _validation_payload(requested_task_result_ids: tuple[int, ...]) -> _FakeValidationExecutorPayload:
    return _FakeValidationExecutorPayload(
        fields=tuple(
            Fields(
                fields={
                    "task_result_id": ScalarField(value=str(_task_result_id(task_result_id_raw))),
                }
            )
            for task_result_id_raw in requested_task_result_ids
        )
    )


def _validation_success(
    *,
    raw_id: int,
    requested_task_result_ids: tuple[int, ...],
    executor_output: object,
) -> _FakeSuccessfulValidationTaskResult:
    return _FakeSuccessfulValidationTaskResult(
        id=_task_result_id(raw_id),
        executor_payload=_validation_payload(requested_task_result_ids),
        executor_output=executor_output,
    )


def _malformed_validation_success(
    *,
    raw_id: int,
    requested_task_result_ids: tuple[int, ...],
    executor_output: object,
) -> _FakeSuccessfulValidationTaskResult:
    return _FakeSuccessfulValidationTaskResult(
        id=_task_result_id(raw_id),
        executor_payload=SimpleNamespace(
            fields=tuple(
                cast(
                    Any,
                    SimpleNamespace(fields={"task_result_id": str(_task_result_id(task_result_id_raw))}),
                )
                for task_result_id_raw in requested_task_result_ids
            )
        ),
        executor_output=executor_output,
    )


def _run_weighing(
    *,
    mining_successes_previous: tuple[_FakeSuccessfulMiningTaskResult, ...],
    mining_executor_failures_previous: tuple[_FakeExecutorFailureTaskResult, ...] = (),
    mining_successes_current: tuple[_FakeSuccessfulMiningTaskResult, ...] = (),
    validation_successes_previous: tuple[_FakeSuccessfulValidationTaskResult, ...] = (),
    validation_successes_current: tuple[_FakeSuccessfulValidationTaskResult, ...] = (),
) -> tuple[dict[Hotkey, float], _FakeTaskResultStore]:
    store = _FakeTaskResultStore(
        successful_by_task_and_epoch={
            (str(MINING_TASK_NAME), PREVIOUS_EPOCH): mining_successes_previous,
            (str(MINING_TASK_NAME), CURRENT_EPOCH): mining_successes_current,
            (str(VALIDATION_TASK_NAME), PREVIOUS_EPOCH): validation_successes_previous,
            (str(VALIDATION_TASK_NAME), CURRENT_EPOCH): validation_successes_current,
        },
        executor_failures_by_task_and_epoch={
            (str(MINING_TASK_NAME), PREVIOUS_EPOCH): mining_executor_failures_previous,
        },
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
    return {hotkey: float(weight) for hotkey, weight in weights.items()}, store


def test_weighing_reads_successes_and_executor_failures_from_explicit_epoch_queries() -> None:
    _, store = _run_weighing(
        mining_successes_previous=(_mining_success(raw_id=1, hotkey="hk1"),),
        mining_executor_failures_previous=(_mining_executor_failure(raw_id=2, hotkey="hk1"),),
        validation_successes_previous=(
            _validation_success(
                raw_id=101,
                requested_task_result_ids=(1,),
                executor_output=_validation_batch(((1, 80),)),
            ),
        ),
    )

    assert store.calls == [
        ("success", str(MINING_TASK_NAME), PREVIOUS_EPOCH),
        ("executor_failure", str(MINING_TASK_NAME), PREVIOUS_EPOCH),
        ("success", str(VALIDATION_TASK_NAME), PREVIOUS_EPOCH),
        ("success", str(VALIDATION_TASK_NAME), CURRENT_EPOCH),
    ]


def test_weighing_ignores_validation_results_with_untyped_task_result_ids() -> None:
    weights, _ = _run_weighing(
        mining_successes_previous=(_mining_success(raw_id=1, hotkey="hk1"),),
        validation_successes_current=(
            _malformed_validation_success(
                raw_id=601,
                requested_task_result_ids=(1,),
                executor_output=_validation_batch(((1, 100),)),
            ),
            _validation_success(
                raw_id=602,
                requested_task_result_ids=(1,),
                executor_output=_validation_batch(((1, 80),)),
            ),
        ),
    )

    assert weights == {Hotkey("hk1"): 80.0}


def test_weighing_uses_only_previous_epoch_mining_successes() -> None:
    weights, _ = _run_weighing(
        mining_successes_previous=(_mining_success(raw_id=1, hotkey="hk1"),),
        mining_successes_current=(_mining_success(raw_id=2, hotkey="hk1"),),
        validation_successes_current=(
            _validation_success(
                raw_id=201,
                requested_task_result_ids=(1, 2),
                executor_output=_validation_batch(((1, 80), (2, 10))),
            ),
        ),
    )

    assert weights == {Hotkey("hk1"): 80.0}


def test_weighing_uses_validation_successes_from_previous_and_current_epoch() -> None:
    weights, _ = _run_weighing(
        mining_successes_previous=(
            _mining_success(raw_id=1, hotkey="hk1"),
            _mining_success(raw_id=2, hotkey="hk1"),
        ),
        validation_successes_previous=(
            _validation_success(
                raw_id=301,
                requested_task_result_ids=(1,),
                executor_output=_validation_batch(((1, 60),)),
            ),
        ),
        validation_successes_current=(
            _validation_success(
                raw_id=302,
                requested_task_result_ids=(2,),
                executor_output=_validation_batch(((2, 90),)),
            ),
        ),
    )

    assert weights == {Hotkey("hk1"): 150.0}


def test_weighing_counts_mining_executor_failures_as_zero_scores() -> None:
    weights, _ = _run_weighing(
        mining_successes_previous=(_mining_success(raw_id=1, hotkey="hk1"),),
        mining_executor_failures_previous=(_mining_executor_failure(raw_id=2, hotkey="hk1"),),
        validation_successes_current=(
            _validation_success(
                raw_id=401,
                requested_task_result_ids=(1,),
                executor_output=_validation_batch(((1, 100),)),
            ),
        ),
    )

    assert weights == {Hotkey("hk1"): 100.0}


def test_weighing_excludes_success_without_validation_from_average() -> None:
    weights, _ = _run_weighing(
        mining_successes_previous=(
            _mining_success(raw_id=1, hotkey="hk1"),
            _mining_success(raw_id=2, hotkey="hk1"),
        ),
        validation_successes_current=(
            _validation_success(
                raw_id=501,
                requested_task_result_ids=(1,),
                executor_output=_validation_batch(((1, 80),)),
            ),
        ),
    )

    assert weights == {Hotkey("hk1"): 160.0}


def test_weighing_ignores_singleton_validation_result_with_extra_unrelated_id() -> None:
    weights, _ = _run_weighing(
        mining_successes_previous=(
            _mining_success(raw_id=1, hotkey="hk1"),
            _mining_success(raw_id=2, hotkey="hk2"),
        ),
        validation_successes_current=(
            _validation_success(
                raw_id=601,
                requested_task_result_ids=(1,),
                executor_output=_validation_batch(((1, 80), (2, 100))),
            ),
        ),
    )

    assert weights == {
        Hotkey("hk1"): 0.0,
        Hotkey("hk2"): 0.0,
    }


def test_weighing_ignores_singleton_validation_result_missing_requested_id() -> None:
    weights, _ = _run_weighing(
        mining_successes_previous=(
            _mining_success(raw_id=1, hotkey="hk1"),
            _mining_success(raw_id=2, hotkey="hk2"),
        ),
        validation_successes_current=(
            _validation_success(
                raw_id=701,
                requested_task_result_ids=(1,),
                executor_output=_validation_batch(((2, 100),)),
            ),
        ),
    )

    assert weights == {
        Hotkey("hk1"): 0.0,
        Hotkey("hk2"): 0.0,
    }
