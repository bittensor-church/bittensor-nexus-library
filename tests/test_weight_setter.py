# pyright: basic
from collections.abc import Mapping
from unittest.mock import create_autospec, seal

import pytest
from pylon_client.artanis import PylonMisconfigured, PylonResponseException
from utils import (
    CollectorActor,
    InMemoryTestTaskResultStoreProvider,
    build_nexus_task_result,
    empty_context_store,
    store_successful_task_result,
    wait_until,
)

from nexus.v1 import (
    BlockNumber,
    EpochBeat,
    Flow,
    Hotkey,
    IdentityPylonApiLike,
    NetUid,
    NexusException,
    NexusTaskName,
    PylonClientProvider,
    SendEvent,
    Source,
    SubnetBuilder,
    SyncPylonClientLike,
    WeighingFunc,
    Weight,
    WeightsCalculationBundle,
    WeightSetterNode,
    WeightSettingException,
    WeightSettingSuccess,
    get_epoch_containing_block,
)

NETUID = NetUid(1)
EPOCH = get_epoch_containing_block(BlockNumber(500), netuid=NETUID)
TASK_NAME = NexusTaskName("test-weight-setter-task")

type DummyExecutorPayload = str
type DummyExecutorOutput = int


def _raise(exc: Exception) -> Mapping[Hotkey, Weight]:
    """
    A lambda cannot raise, hence this helper.
    """
    raise exc


def _weigh_by_task_result_count(task_name: NexusTaskName) -> WeighingFunc:
    def _weigh(bundle: WeightsCalculationBundle) -> Mapping[Hotkey, Weight]:
        counts = bundle.tasks_result_store.count_successful_by_hotkey_for_epoch(task_name=task_name, epoch=bundle.epoch)
        return {hotkey: Weight(float(count)) for hotkey, count in counts.items()}

    return _weigh


def _seed_results_across_epochs(
    task_result_store_provider: InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput, str],
    entries: tuple[tuple[int, str], ...],
) -> None:
    task_result_store = task_result_store_provider.get_task_result_store()
    context_store = empty_context_store()

    for block_number, hotkey in entries:
        result = build_nexus_task_result(
            executor_payload=f"input-{block_number}",
            output=block_number,
            block_number=block_number,
            target_hotkey=hotkey,
        )
        store_successful_task_result(
            context_store=context_store,
            task_result_store=task_result_store,
            task_name=TASK_NAME,
            result=result,
            executor_public_output=f"public-{block_number}",
        )


@pytest.fixture
def mock_pylon_client():
    client = create_autospec(spec=SyncPylonClientLike, instance=True)
    client.identity = create_autospec(spec=IdentityPylonApiLike, instance=True)
    client.__enter__.return_value = client
    client.__exit__.return_value = None
    seal(client)
    return client


def _build_and_run(
    *,
    weighing_func: WeighingFunc,
    pylon_client: SyncPylonClientLike,
    task_result_store_provider: InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput, str],
) -> tuple[list[WeightSettingSuccess], list[NexusException]]:
    provider = create_autospec(spec=PylonClientProvider, instance=True)
    provider.get_client.return_value = pylon_client
    seal(provider)

    trigger = Source[EpochBeat]("test-trigger")
    node = WeightSetterNode(
        "test-weight-setter",
        weighing_func=weighing_func,
        pylon_client_provider=provider,
        task_result_store_provider=task_result_store_provider,
    )

    builder = SubnetBuilder(nodes=[node])
    ok_collector = CollectorActor[WeightSettingSuccess](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="ok-collector",
    )
    error_collector = CollectorActor[NexusException](
        pipe_to_bus=builder.pipe_to_bus,
        context_store=builder.context_store,
        name="error-collector",
    )

    flow = Flow.from_connectable(trigger).then(node).then(ok=ok_collector.sink, error=error_collector.sink)

    runtime = builder.add_flows(flow).add_actors(ok_collector, error_collector).build()

    with runtime.running(shutdown_timeout_seconds=1.0):
        with builder.context_store.create_context() as ctx:
            pass
        builder.pipe_to_bus.put(SendEvent(ctx_id=ctx.id, source=trigger, payload=EpochBeat(epoch=EPOCH)))
        wait_until(lambda: len(ok_collector.received_events) + len(error_collector.received_events) >= 1)

    return (
        [e.payload for e in ok_collector.received_events],
        [e.payload for e in error_collector.received_events],
    )


def test_happy_path_sets_weights_and_emits_success(mock_pylon_client):
    store_provider = InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput, str]()

    # EPOCH for block=500 and netuid=1 is 358..718. Only these should be counted.
    entries = (
        (200, "hk1"),  # before epoch
        (357, "hk3"),  # just before epoch (excluded)
        (358, "hk1"),  # epoch first block (included)
        (400, "hk1"),  # in epoch
        (500, "hk1"),  # in epoch
        (700, "hk2"),  # in epoch
        (718, "hk2"),  # epoch last block (included)
        (719, "hk3"),  # just after epoch (excluded)
        (800, "hk3"),  # after epoch
    )
    _seed_results_across_epochs(store_provider, entries)

    ok, errors = _build_and_run(
        weighing_func=_weigh_by_task_result_count(TASK_NAME),
        pylon_client=mock_pylon_client,
        task_result_store_provider=store_provider,
    )

    assert errors == []
    assert ok == [WeightSettingSuccess()]
    mock_pylon_client.identity.put_weights.assert_called_once_with(
        {
            Hotkey("hk1"): Weight(3.0),
            Hotkey("hk2"): Weight(2.0),
        }
    )


def test_weighing_failure_emits_error(mock_pylon_client):
    store_provider = InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput, str]()
    ok, errors = _build_and_run(
        weighing_func=lambda bundle: _raise(Exception("calculation exploded")),
        pylon_client=mock_pylon_client,
        task_result_store_provider=store_provider,
    )
    assert [type(e) for e in errors] == [WeightSettingException]
    assert ok == []
    mock_pylon_client.identity.put_weights.assert_not_called()


def test_pylon_failure_emits_error(mock_pylon_client):
    store_provider = InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput, str]()

    # EPOCH for block=500 and netuid=1 is 358..718. Only these should be counted.
    entries = (
        (200, "hk1"),  # before epoch
        (400, "hk1"),  # in epoch
        (500, "hk1"),  # in epoch
        (700, "hk2"),  # in epoch
        (800, "hk3"),  # after epoch
    )
    _seed_results_across_epochs(store_provider, entries)

    mock_pylon_client.identity.put_weights.side_effect = PylonResponseException("pylon is not cooperating")
    ok, errors = _build_and_run(
        weighing_func=_weigh_by_task_result_count(TASK_NAME),
        pylon_client=mock_pylon_client,
        task_result_store_provider=store_provider,
    )
    assert [type(e) for e in errors] == [WeightSettingException]
    assert ok == []


def test_pylon_identity_misconfigured_emits_error(mock_pylon_client):
    store_provider = InMemoryTestTaskResultStoreProvider[DummyExecutorPayload, DummyExecutorOutput, str]()
    _seed_results_across_epochs(
        store_provider,
        entries=((400, "hk1"),),
    )

    mock_pylon_client.identity.put_weights.side_effect = PylonMisconfigured(
        "Can not use identity api - no identity name or token provided in config."
    )
    ok, errors = _build_and_run(
        weighing_func=_weigh_by_task_result_count(TASK_NAME),
        pylon_client=mock_pylon_client,
        task_result_store_provider=store_provider,
    )
    assert [type(e) for e in errors] == [WeightSettingException]
    assert ok == []
