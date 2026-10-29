"""Service wiring must admit database writes before they contend for the pool."""

import asyncio
import contextlib
import json
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_entity_queues_share_admission_and_cancel_safely(monkeypatch) -> None:
    from brainztableinator import brainztableinator as service

    cfg = MagicMock()
    cfg.postgres_pool_max_size = 2
    monkeypatch.setattr(service, "config", cfg)
    monkeypatch.setattr(service, "shutdown_requested", False)
    monkeypatch.setattr(service, "_write_slots", None)
    active = peak = 0
    entered = asyncio.Event()
    release = asyncio.Event()

    @contextlib.asynccontextmanager
    async def acquire():
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 2:
            entered.set()
        try:
            await release.wait()
            yield MagicMock()
        finally:
            active -= 1

    conn = MagicMock()
    conn.set_autocommit = AsyncMock()
    conn.transaction.return_value = AsyncMock()

    @contextlib.asynccontextmanager
    async def connection():
        async with acquire():
            yield conn

    pool = MagicMock()
    pool.connection.side_effect = connection
    monkeypatch.setattr(service, "connection_pool", pool)
    monkeypatch.setattr(service, "PROCESSORS", {kind: AsyncMock() for kind in service.PROCESSORS})
    kinds = ["artists", "labels", "release-groups", "releases"]

    messages = []
    for _ in range(9):
        message = MagicMock()
        message.body = json.dumps({"id": str(uuid4()), "sha256": "hash", "name": "record"}).encode()
        message.headers = {}
        message.ack = AsyncMock()
        message.nack = AsyncMock()
        messages.append(message)
    tasks = [asyncio.create_task(service.on_data_message(message, kinds[i % 4])) for i, message in enumerate(messages[:8])]
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        await asyncio.sleep(0)
        assert active == peak == 2
        # A broker reconnect must retain this same budget while old writes drain.
        gate = service._write_admission()
        assert service._write_admission() is gate
        tasks[-1].cancel()
        tasks[0].cancel()
        results = await asyncio.gather(tasks[-1], tasks[0], return_exceptions=True)
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks[1:-1]), timeout=2)
        await asyncio.wait_for(service.on_data_message(messages[-1], kinds[0]), timeout=2)
        async with asyncio.timeout(2), gate.slot(), gate.slot():
            assert active == 0 and peak == 2
        for index, message in enumerate(messages):
            assert message.ack.await_count == (0 if index in (0, 7) else 1)
            message.nack.assert_not_awaited()
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
