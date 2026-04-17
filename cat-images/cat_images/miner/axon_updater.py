import logging
import threading
from collections.abc import Callable, Generator
from concurrent.futures import BrokenExecutor, Executor, ProcessPoolExecutor, TimeoutError
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta

from nexus.utils.types import NetUid, Port

log = logging.getLogger("axon_updater")


@dataclass(frozen=True)
class AxonUpdaterConfig:
    wallet_name: str
    hotkey_name: str
    subtensor_network: str
    netuid: NetUid
    port: Port
    external_ip: str | None
    external_port: Port | None
    interval: timedelta


def _default_executor_factory() -> Executor:
    import multiprocessing  # noqa: PLC0415

    # We need to "spawn" to give the worker process a clean slate.
    # By default, a fork would let bittensor's LoggingMachine destroy the main process loggers.
    return ProcessPoolExecutor(
        max_workers=1,
        max_tasks_per_child=1,
        mp_context=multiprocessing.get_context("spawn"),
    )


def _update_axon_subprocess(config: AxonUpdaterConfig) -> tuple[bool, str]:
    """Disposable subprocess task — imports bittensor, checks chain, serves if needed, then dies.

    This function runs exactly once per spawned process (max_tasks_per_child=1).
    The bittensor SDK is imported here to keep it out of the main process.
    No logging — bittensor destroys subprocess loggers on import. Results are
    communicated back to the main process via return value (plain picklable types only).
    """
    try:
        import bittensor as bt  # noqa: PLC0415 — must stay out of main process
        from bittensor.utils.networking import get_external_ip  # noqa: PLC0415

        bt.logging.off()

        wallet = bt.Wallet(name=config.wallet_name, hotkey=config.hotkey_name)
        subtensor = bt.Subtensor(network=config.subtensor_network)
        hotkey_ss58 = wallet.hotkey.ss58_address
        external_ip = config.external_ip or get_external_ip()
        external_port = config.external_port or config.port

        neuron = subtensor.get_neuron_for_pubkey_and_subnet(hotkey_ss58, config.netuid)
        axon_info = neuron.axon_info

        HTTP_PROTOCOL = 4

        if axon_info is not None and axon_info.ip != "0.0.0.0":
            current_protocol = axon_info.protocol
            if axon_info.ip == external_ip and axon_info.port == external_port and current_protocol == HTTP_PROTOCOL:
                return False, f"Axon unchanged at {external_ip}:{external_port}"

        axon = bt.Axon(wallet=wallet, port=config.port, external_ip=external_ip, external_port=external_port)
        subtensor.serve_axon(netuid=config.netuid, axon=axon)
        return True, f"Axon updated to {external_ip}:{external_port}"
    except KeyboardInterrupt:
        return False, "Axon update interrupted"
    except Exception as exc:
        return False, f"Axon update failed: {type(exc).__name__}: {exc}"


class AxonUpdaterService:
    """Periodically checks on-chain axon info and re-serves if IP/port drifted.

    Delegates all bittensor SDK work to an executor (by default a disposable subprocess via
    ProcessPoolExecutor with max_tasks_per_child=1) so the SDK is never imported in the main process.

    Args:
        config: Chain connection and serving details (wallet, subtensor_network, netuid, port, interval).
        executor_factory: Callable that returns an `Executor` instance. Defaults to a single-worker
            ProcessPoolExecutor. Pass e.g. a ThreadPoolExecutor factory for testing.
    """

    _SUBPROCESS_TIMEOUT_SECONDS = 120

    _config: AxonUpdaterConfig
    _executor_factory: Callable[[], Executor]
    _pool: Executor | None
    _stop_event: threading.Event
    _thread: threading.Thread | None

    def __init__(
        self,
        config: AxonUpdaterConfig,
        executor_factory: Callable[[], Executor] | None = None,
    ) -> None:
        self._config = config
        self._executor_factory = executor_factory or _default_executor_factory
        self._pool = None
        self._stop_event = threading.Event()
        self._thread = None

    def start(self) -> None:
        self._pool = self._executor_factory()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        log.info(f"AxonUpdaterService started (interval={self._config.interval})")

    def _loop(self) -> None:
        pool = self._pool
        if pool is None:
            raise RuntimeError("_loop() called before start()")
        while True:
            log.info(f"Checking axon info for netuid={self._config.netuid}")
            try:
                ok, msg = pool.submit(_update_axon_subprocess, self._config).result(
                    timeout=self._SUBPROCESS_TIMEOUT_SECONDS
                )
                log.info(msg) if ok else log.warning(msg)
            except BrokenExecutor:
                log.warning("Process pool terminated, shutting down")
                break
            except TimeoutError:
                log.error(f"Axon update timed out after {self._SUBPROCESS_TIMEOUT_SECONDS}s")
            if self._stop_event.wait(self._config.interval.total_seconds()):
                break

    def stop(self) -> None:
        self._stop_event.set()
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)
            self._pool = None
        if self._thread is not None:
            self._thread.join(timeout=self._SUBPROCESS_TIMEOUT_SECONDS + 5)
            self._thread = None
        log.info("AxonUpdaterService stopped")

    @contextmanager
    def running(self) -> Generator[None]:
        self.start()
        try:
            yield
        finally:
            self.stop()
