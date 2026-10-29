"""Deterministic coverage for shared database admission across entity consumers."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common import WriteAdmission

import brainztableinator.brainztableinator as service


VALID_BODY = b'{"id":"550e8400-e29b-41d4-a716-446655440000","name":"Test"}'


def _message() -> Any:
    message = MagicMock()
    message.body = VALID_BODY
    return message


def _connection() -> AsyncMock:
    connection = AsyncMock()
    transaction = AsyncMock()
    transaction.__aenter__ = AsyncMock(return_value=transaction)
    transaction.__aexit__ = AsyncMock(return_value=False)
    connection.transaction = MagicMock(return_value=transaction)
    return connection


@pytest.mark.asyncio
async def test_mixed_entity_writes_share_the_pool_bound() -> None:
    """Four entity handlers use one capacity-two budget before pool acquisition."""
    active = 0
    peak = 0
    entered = asyncio.Event()
    release = asyncio.Event()
    pool = MagicMock()

    @asynccontextmanager
    async def connection():
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        entered.set()
        try:
            await release.wait()
            yield _connection()
        finally:
            active -= 1

    pool.connection = connection
    processor = AsyncMock()
    with (
        patch.object(service, "connection_pool", pool),
        patch.object(service, "write_admission", WriteAdmission(2)),
        patch.dict(service.PROCESSORS, dict.fromkeys(service.MUSICBRAINZ_DATA_TYPES, processor)),
    ):
        tasks = [asyncio.create_task(service._handle_data_message(_message(), data_type)) for data_type in service.MUSICBRAINZ_DATA_TYPES]
        await asyncio.wait_for(entered.wait(), timeout=1)
        await asyncio.sleep(0)
        assert active == 2
        assert peak == 2
        release.set()
        results = await asyncio.gather(*tasks)

    assert peak == 2
    assert all(result.outcome == "processed" for result in results)
    assert processor.await_count == len(service.MUSICBRAINZ_DATA_TYPES)


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_consume_or_leak_a_permit() -> None:
    admission = WriteAdmission(1)
    started = asyncio.Event()
    finish = asyncio.Event()

    async def holder() -> None:
        async with admission.slot():
            started.set()
            await finish.wait()

    first = asyncio.create_task(holder())
    await started.wait()
    waiter = asyncio.create_task(holder())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    finish.set()
    await first
    async with asyncio.timeout(1):
        async with admission.slot():
            pass


@pytest.mark.asyncio
async def test_cancelled_database_work_releases_admission() -> None:
    active = 0
    pool = MagicMock()

    @asynccontextmanager
    async def connection():
        nonlocal active
        active += 1
        try:
            yield _connection()
        finally:
            active -= 1

    pool.connection = connection
    entered = asyncio.Event()

    async def processor(_connection: Any, _data: dict[str, Any]) -> None:
        entered.set()
        await asyncio.Future()

    with (
        patch.object(service, "connection_pool", pool),
        patch.object(service, "write_admission", WriteAdmission(1)),
        patch.dict(service.PROCESSORS, {"artists": processor}),
    ):
        task = asyncio.create_task(service._handle_data_message(_message(), "artists"))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert active == 0
        async with asyncio.timeout(1):
            async with service.write_admission.slot():  # type: ignore[union-attr]
                pass


@pytest.mark.asyncio
async def test_handler_failure_releases_admission_for_next_entity() -> None:
    pool = MagicMock()
    pool.connection = MagicMock()
    connection = _connection()
    context = AsyncMock()
    context.__aenter__.return_value = connection
    context.__aexit__.return_value = False
    pool.connection.return_value = context
    failing = AsyncMock(side_effect=RuntimeError("write failed"))
    succeeding = AsyncMock()
    with (
        patch.object(service, "connection_pool", pool),
        patch.object(service, "write_admission", WriteAdmission(1)),
        patch.dict(service.PROCESSORS, {"artists": failing, "labels": succeeding}),
    ):
        with pytest.raises(RuntimeError, match="write failed"):
            await service._handle_data_message(_message(), "artists")
        result = await service._handle_data_message(_message(), "labels")

    assert result.outcome == "processed"
    succeeding.assert_awaited_once()


@pytest.mark.asyncio
async def test_missing_write_admission_fails_before_pool_checkout() -> None:
    pool = MagicMock()
    with (
        patch.object(service, "connection_pool", pool),
        patch.object(service, "write_admission", None),
        pytest.raises(RuntimeError, match="Write admission not initialized"),
    ):
        await service._handle_data_message(_message(), "artists")

    pool.connection.assert_not_called()


@pytest.mark.asyncio
async def test_negative_control_without_handler_admission_exceeds_pool_budget() -> None:
    """Demonstrate why global-only QoS cannot bound writes across separate consumers."""
    active = 0
    peak = 0
    started = asyncio.Event()
    release = asyncio.Event()

    @asynccontextmanager
    async def unguarded_connection():
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        started.set()
        try:
            await release.wait()
            yield _connection()
        finally:
            active -= 1

    class NoAdmission:
        @staticmethod
        @asynccontextmanager
        async def slot():
            yield

        @staticmethod
        @asynccontextmanager
        async def handler():
            yield

        @staticmethod
        async def drain(*, timeout: float) -> bool:  # noqa: ARG004
            return True

    pool = MagicMock(connection=unguarded_connection)

    async def processor(_connection: Any, _data: dict[str, Any]) -> None:
        await asyncio.sleep(0)

    with (
        patch.object(service, "connection_pool", pool),
        patch.object(service, "write_admission", NoAdmission()),
        patch.dict(service.PROCESSORS, dict.fromkeys(service.MUSICBRAINZ_DATA_TYPES, processor)),
    ):
        tasks = [asyncio.create_task(service._handle_data_message(_message(), data_type)) for data_type in service.MUSICBRAINZ_DATA_TYPES]
        await started.wait()
        await asyncio.sleep(0)
        assert peak == 4  # Per-consumer prefetch allows four independent consumers in flight.
        release.set()
        await asyncio.gather(*tasks)

    assert peak > 2


@pytest.mark.asyncio
async def test_consumer_qos_uses_supported_per_consumer_scope() -> None:
    channel = AsyncMock()
    with patch.object(service, "config", MagicMock(postgres_pool_max_size=3)):
        await service._configure_consumer_qos(channel)
    channel.set_qos.assert_awaited_once_with(prefetch_count=3, global_=False)


@pytest.mark.asyncio
async def test_shutdown_drain_waits_for_callbacks_started_by_consumer_handler() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    admission = WriteAdmission(2)

    async def blocked(_message: Any, _data_type: str) -> Any:
        started.set()
        await release.wait()

    with patch.object(service, "on_data_message", blocked), patch.object(service, "write_admission", admission):
        callback = asyncio.create_task(service.make_data_handler("artists")(_message()))
        await started.wait()
        drain = asyncio.create_task(admission.drain(timeout=1))
        await asyncio.sleep(0)
        assert not drain.done()
        release.set()
        await callback
        assert await drain is True
