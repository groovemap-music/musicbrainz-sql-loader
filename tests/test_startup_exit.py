"""The cli entry point exits 1 when startup gives up and 0 on a signal-driven shutdown."""

import os
import subprocess
import sys
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import brainztableinator.brainztableinator as service


if TYPE_CHECKING:
    from collections.abc import Iterator


MODULE = "brainztableinator.brainztableinator"


@pytest.fixture
def started() -> Iterator[dict[str, Any]]:
    """Patch everything main() touches before the AMQP loop; expose the doubles."""
    health = MagicMock()
    pool = MagicMock()
    pool.initialize = AsyncMock()
    pool.close = AsyncMock()
    manager = AsyncMock()
    with (
        patch.dict(os.environ, {"STARTUP_DELAY": "0"}),
        patch(f"{MODULE}.setup_logging"),
        patch(f"{MODULE}.setup_telemetry"),
        patch(f"{MODULE}.start_event_loop_monitor"),
        patch(f"{MODULE}.shutdown_telemetry") as shutdown_telemetry,
        patch(f"{MODULE}.HealthServer", return_value=health),
        patch(f"{MODULE}.AsyncPostgreSQLPool", return_value=pool),
        patch(f"{MODULE}.AsyncResilientRabbitMQ", return_value=manager),
        patch(f"{MODULE}.MusicBrainzSQLLoaderConfig") as config_class,
        patch(f"{MODULE}.reconciliation_columns_present", AsyncMock(return_value=False)),
        patch(f"{MODULE}.shutdown_requested", False),
        patch("asyncio.sleep", AsyncMock()),
    ):
        config_class.from_env.return_value = MagicMock(
            health_port=8000,
            postgres_host="localhost:5432",
            postgres_pool_max_size=4,
            postgres_pool_min_size=1,
        )
        yield {
            "health": health,
            "pool": pool,
            "manager": manager,
            "shutdown_telemetry": shutdown_telemetry,
            "config_class": config_class,
        }


def _assert_exit_1(started: dict[str, Any]) -> None:
    with pytest.raises(SystemExit) as exc:
        service.cli()
    assert exc.value.code == 1
    started["shutdown_telemetry"].assert_called_once()


def test_cli_exits_1_on_configuration_error(started: dict[str, Any]) -> None:
    started["config_class"].from_env.side_effect = ValueError("Missing env vars")
    _assert_exit_1(started)


def test_cli_exits_1_when_pool_initialization_fails(started: dict[str, Any]) -> None:
    started["pool"].initialize.side_effect = Exception("Cannot create pool")
    _assert_exit_1(started)
    started["health"].stop.assert_called_once()


def test_cli_exits_1_when_amqp_broker_unreachable(started: dict[str, Any]) -> None:
    started["manager"].connect.side_effect = Exception("Cannot connect")
    _assert_exit_1(started)
    started["health"].stop.assert_called_once()
    started["pool"].close.assert_awaited_once()


def test_cli_exits_1_when_no_amqp_connection_is_returned(started: dict[str, Any]) -> None:
    started["manager"].connect.return_value = None
    _assert_exit_1(started)
    started["pool"].close.assert_awaited_once()


def test_cli_exits_0_when_signalled_before_amqp_connects(started: dict[str, Any]) -> None:
    async def signalled(*_args: Any, **_kwargs: Any) -> None:
        service.shutdown_requested = True
        raise Exception("interrupted")

    started["manager"].connect.side_effect = signalled
    try:
        service.cli()
    finally:
        service.shutdown_requested = False
    started["shutdown_telemetry"].assert_called_once()


def test_cli_exits_0_on_signal_driven_shutdown(started: dict[str, Any]) -> None:
    connection = MagicMock()
    connection.__aenter__ = AsyncMock(return_value=connection)
    connection.__aexit__ = AsyncMock(return_value=None)
    channel = AsyncMock()
    connection.channel = AsyncMock(return_value=channel)
    queue = AsyncMock()
    channel.declare_queue.return_value = queue
    started["manager"].connect.return_value = connection

    async def stop_after_first_wait(coro: Any, timeout: float) -> None:  # noqa: ARG001
        coro.close()
        service.shutdown_requested = True
        raise TimeoutError

    try:
        with (
            patch("asyncio.wait_for", stop_after_first_wait),
            patch(f"{MODULE}.progress_reporter", AsyncMock()),
            patch(f"{MODULE}.periodic_queue_checker", AsyncMock()),
        ):
            service.cli()  # returns normally: exit status 0
    finally:
        service.shutdown_requested = False
    started["shutdown_telemetry"].assert_called_once()


def _run_cli_process(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    base = {k: v for k, v in os.environ.items() if k.startswith(("PATH", "PYTHON", "HOME", "VIRTUAL_ENV"))}
    return subprocess.run(
        [sys.executable, "-c", "from brainztableinator.brainztableinator import cli; cli()"],
        env={**base, "STARTUP_DELAY": "0", **env},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_process_exits_1_with_missing_required_environment() -> None:
    result = _run_cli_process({})
    assert result.returncode == 1, result.stderr + result.stdout


def test_process_exits_1_with_invalid_health_port() -> None:
    result = _run_cli_process({"HEALTH_PORT": "abc"})
    assert result.returncode == 1, result.stderr + result.stdout
