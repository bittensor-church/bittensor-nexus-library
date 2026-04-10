# Split Task Results Into Success And Executor-Failure Types

## Goal

Replace the flag-based `SingleTaskResult` model with two concrete task-result types so downstream actors express whether they consume successes or executor failures in their type signatures and wiring.

This is a clean-slate change. No backward compatibility layer is kept.

## Decision Summary

- Delete `SingleTaskResult`.
- Introduce:
  - `SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]`
  - `ExecutorFailureTaskResult[ExecutorPayload]`
- Make `NexusTask` expose separate typed sources:
  - `successful_task_result`
  - `executor_failure`
  - `executor_output`
  - `error`
- Keep `error` reserved for framework failures and terminal retry exhaustion.
- Persist executor failures as task results, but do not represent framework failures as task results.
- Replace mixed epoch queries and boolean filtering flags with explicit success/failure store APIs.

## Why This Change

The current `SingleTaskResult` shape mixes two incompatible states:

- a successful execution with real `executor_output` and `executor_public_output`
- an executor-side failure stored as `NexusException`

That forces downstream code to inspect `is_failure`, check `executor_public_output is None`, or narrow `executor_output` manually. The current OpenRouter selection helpers, `TaskInputOutputCreator`, and weighing logic already show this pressure.

The design goal is to make invalid uses unrepresentable:

- success-only actors should not accept failure results at all
- failure-oriented actors should not pretend they can receive successful outputs
- validator wiring should make the consumer's intent visible at the connection site

## Type Model

Use one shared base plus two leaf types.

```python
@dataclass(frozen=True)
class TaskResultBase[ExecutorPayload]:
    id: TaskResultId
    processing_started: datetime
    processing_finished: datetime
    block_at_finish: BlockBeat
    executor_payload: ExecutorPayload
    target: Neuron


@dataclass(frozen=True)
class SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](
    TaskResultBase[ExecutorPayload]
):
    executor_output: ExecutorOutput
    executor_public_output: ExecutorPublicOutput


@dataclass(frozen=True)
class ExecutorFailureTaskResult[ExecutorPayload](
    TaskResultBase[ExecutorPayload]
):
    executor_failure: ExecutorFailureException
```

### Notes

- `SuccessfulTaskResult.executor_public_output` is required. A successful task result without public output is invalid.
- `ExecutorFailureTaskResult` stores `ExecutorFailureException`, not arbitrary `NexusException`.
- `is_failure` is removed. The class is the discriminator.
- Public task-result types are flat. External code should not navigate nested transport wrappers such as `result.executor_output.input.input`.

## Naming

Use `executor_failure` as the public task source name instead of `failed_task_result`.

That source carries `ExecutorFailureTaskResult[ExecutorPayload]`. The payload type keeps the "task result" meaning explicit, while the source name matches the executor-failure semantics the task is exposing.

## NexusTask Public API

`NexusTask` should expose these public outputs:

- `successful_task_result: Source[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]`
- `executor_failure: Source[ExecutorFailureTaskResult[ExecutorPayload]]`
- `executor_output: Source[ExecutorPublicOutput]`
- `error: Source[NexusException]`

### Semantics

- `successful_task_result`
  - emitted on a child context
  - represents a persisted successful execution record
- `executor_failure`
  - emitted on a child context
  - represents a persisted executor-failure execution record
- `executor_output`
  - emitted on the parent context
  - carries only successful `ExecutorPublicOutput`
- `error`
  - emitted on the parent context
  - carries framework/internal failures and terminal retry exhaustion only

This keeps the existing distinction between durable task-result branches and immediate parent-context side effects.

## Persistence And Store API

The task-result store should become explicit about result kind.

```python
class TaskResultStore[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput](ABC):
    @abstractmethod
    def add_successful_task_result(
        self,
        ctx: Context,
        task_name: NexusTaskName,
        result: SuccessfulTaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput],
    ) -> SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]:
        ...

    @abstractmethod
    def add_executor_failure(
        self,
        ctx: Context,
        task_name: NexusTaskName,
        result: ExecutorFailureToPersist[ExecutorPayload],
    ) -> ExecutorFailureTaskResult[ExecutorPayload]:
        ...

    @abstractmethod
    def get_task_result(
        self,
        task_name: NexusTaskName,
        task_result_id: TaskResultId,
    ) -> SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput] | ExecutorFailureTaskResult[ExecutorPayload]:
        ...

    @abstractmethod
    def get_successful_tasks_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
    ) -> tuple[SuccessfulTaskResult[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput], ...]:
        ...

    @abstractmethod
    def get_executor_failures_for_epoch(
        self,
        task_name: NexusTaskName,
        epoch: Epoch,
    ) -> tuple[ExecutorFailureTaskResult[ExecutorPayload], ...]:
        ...
```

### Query Rules

- Epoch queries are split by result kind.
- Mixed `get_tasks_for_epoch(...)` is removed.
- `get_task_result(...)` may return a union because id-based lookup is the one boundary where the caller may not know the kind in advance.

### Counting

Replace boolean flags such as `include_executor_failures` with explicit methods:

- `count_successful_by_hotkey_for_epoch(...)`
- `count_executor_failures_by_hotkey_for_epoch(...)`
- `count_attempts_by_hotkey_for_epoch(...)` if a caller truly needs both

The API should never ask callers to control semantic categories via booleans.

## Internal Pipeline Design

The pipeline already distinguishes executor-side failures from framework failures. The redesign should make that split explicit in typed actor outputs.

### TaskResultPreparer

Keep one `TaskResultPreparer` actor because it already coordinates the temporary pending-success state while public output is being converted.

Change its outputs to:

- `executor_output_for_conversion: Source[ExecutorOutput]`
- `prepared_successful_task_result: Source[SuccessfulTaskResultToPersist[ExecutorPayload, ExecutorOutput, ExecutorPublicOutput]]`
- `prepared_executor_failure: Source[ExecutorFailureToPersist[ExecutorPayload]]`
- `error: Source[NexusException]`

Behavior:

- if the timestamped executor result contains `ExecutorFailureException`, emit `prepared_executor_failure`
- if it contains a successful executor output, store the pending timestamped result and emit `executor_output_for_conversion`
- when the converted public output arrives, emit `prepared_successful_task_result`
- if conversion fails, clear the pending success and emit no task result

### Storers

Split storage into two actors:

- `SuccessfulTaskResultStorer`
- `ExecutorFailureTaskResultStorer`

Each actor has one input type and one output type. Neither actor accepts a mixed union.

`ExecutorFailureTaskResultStorer` should preserve the current retry behavior:

- persist the executor failure
- raise `RetryTaskAfterExecutorFailureException`

That means executor failures remain visible in the store even when the task later succeeds on retry.

### Dispatcher

Replace `TaskResultSplitter` with a dispatcher that routes typed persisted results.

Recommended shape:

- sinks:
  - `successful_task_result_input`
  - `executor_failure_input`
- sources:
  - `successful_task_result`
  - `executor_failure`
  - `executor_output`

Behavior:

- create a child context for each persisted task-result event
- emit the typed task result on that child context
- for successful results, also emit `executor_output` on the parent context
- do not emit executor failures on `executor_output`

No separate raw executor-failure side channel is added yet. The typed `executor_failure` task-result branch is sufficient for the current use case.

## Error Model

Three categories must stay distinct.

### 1. Successful execution

- persisted as `SuccessfulTaskResult`
- emitted on `successful_task_result`
- emits `executor_output`

### 2. Executor-side failure

- persisted as `ExecutorFailureTaskResult`
- emitted on `executor_failure`
- triggers retry through `RetryTaskAfterExecutorFailureException`

### 3. Framework/internal failure

Examples:

- payload creator failure
- router failure
- communicator internal error
- public-output conversion failure
- terminal retries exhausted

These are not task results.

They flow only through retry and then `task.error` if the task terminates in failure.

## Downstream Actor Changes

Downstream actors should encode success/failure interest in their input types.

### Success-only actors

- `TaskResultSampler` becomes `SuccessfulTaskResultSampler`
- `TaskInputOutputCreator` accepts only `tuple[SuccessfulTaskResult[..., ..., ...], ...]`
- OpenRouter task-result selectors accept `SuccessfulTaskResult`
- validator-side selection helpers stop checking `is_failure`
- `executor_public_output is None` checks disappear from success-path code

Example validator wiring:

```python
self.connect(self.mining_task.successful_task_result, self.miner_result_sampler.task_results)
self.connect(self.miner_result_sampler.sampled_batch, self.validation_task.input)
```

### Failure-oriented actors

If a consumer wants executor failures, it should declare that directly:

```python
self.connect(self.mining_task.executor_failure, self.failure_auditor.task_results)
```

### Consumers that need both

Consumers such as weighing should combine success and failure explicitly instead of receiving a mixed type by default.

For store-based reads:

```python
mining_successes = store.get_successful_tasks_for_epoch(...)
mining_failures = store.get_executor_failures_for_epoch(...)
```

That makes policy decisions obvious at the call site.

## Cat-Images Impact

The current cat-images code becomes simpler.

- `build_validation_item_selector()` accepts `SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult]`
- no `_selectable_task_result()` helper is needed
- `TaskResultSampler` only samples successful mining results
- validation task input becomes `tuple[SuccessfulTaskResult[MinerPayload, MinerResult, MinerPublicResult], ...]`
- weighing logic reads:
  - mining successes explicitly
  - mining executor failures explicitly
  - validation successes explicitly

Validation executor failures can be ignored intentionally instead of being skipped by `is_failure` checks.

## Testing Strategy

Update tests to lock the new semantics.

### Runtime tests

- successful task execution emits exactly:
  - one `successful_task_result`
  - one `executor_output`
  - no `executor_failure`
  - no `error`
- executor-side failure emits:
  - one persisted `ExecutorFailureTaskResult`
  - one `executor_failure`
  - retry signal
  - no `successful_task_result`
  - no `executor_output`
- framework/internal failure emits:
  - no task result
  - no `executor_output`
  - `error` or retry progression as appropriate

### Store tests

- success and executor-failure records persist with the right concrete type
- epoch queries return the right disjoint collections
- mixed counting booleans are gone

### Downstream actor tests

- success-only actors cannot be wired to executor-failure sources by type
- OpenRouter selection helpers work on `SuccessfulTaskResult` without runtime guards
- weighing logic uses explicit success/failure queries

## Documentation Updates

Update all public docs and examples that currently mention `SingleTaskResult`:

- validator authoring recipes
- cat-images walkthrough
- component catalog
- task-result helper docs

The docs should consistently describe:

- `successful_task_result` for successful persisted results
- `executor_failure` for persisted executor-failure results
- `error` for framework failures

## Non-Goals

- backward compatibility aliases
- preserving `SingleTaskResult`
- preserving mixed epoch queries
- preserving boolean query flags that switch between success and failure semantics
