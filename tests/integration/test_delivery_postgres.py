"""Real-PostgreSQL regressions for delivery settlement and transaction ownership."""

from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import psycopg
import pytest
import pytest_asyncio
from common import DeliveryResult, Settlement
from psycopg import sql

import brainztableinator.brainztableinator as service


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = pytest.mark.integration


class StrictDelivery:
    def __init__(self) -> None:
        self.body = b'{"id":"550e8400-e29b-41d4-a716-446655440000","name":"Artist"}'
        self.headers: dict[str, Any] = {}
        self.calls: list[tuple[str, bool | None]] = []

    async def ack(self) -> None:
        self._record("ack", None)

    async def nack(self, *, requeue: bool) -> None:
        self._record("nack", requeue)

    def _record(self, operation: str, requeue: bool | None) -> None:
        if self.calls:
            raise AssertionError(f"second terminal call after {self.calls!r}")
        self.calls.append((operation, requeue))


class RealConnectionPool:
    """Open one real connection per delivery through the production pool protocol."""

    def __init__(self, database_url: str, schema_name: str) -> None:
        self._database_url = database_url
        self._schema_name = schema_name

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        connection = await psycopg.AsyncConnection.connect(self._database_url)
        await connection.set_autocommit(True)
        await connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self._schema_name)))
        try:
            yield connection
        finally:
            if not connection.closed:
                await connection.close()


@pytest_asyncio.fixture
async def postgres_boundary() -> AsyncIterator[tuple[str, str]]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")

    schema_name = f"musicbrainz_sql_loader_test_{uuid.uuid4().hex}"
    connection = await psycopg.AsyncConnection.connect(database_url)
    await connection.set_autocommit(True)
    schema = sql.Identifier(schema_name)
    try:
        await connection.execute(sql.SQL("CREATE SCHEMA {}").format(schema))
        await connection.execute(sql.SQL("CREATE TABLE {}.records (id uuid PRIMARY KEY, name text NOT NULL)").format(schema))
        yield database_url, schema_name
    finally:
        await connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(schema))
        await connection.close()


async def _row_count(database_url: str, schema_name: str) -> int:
    connection = await psycopg.AsyncConnection.connect(database_url)
    try:
        cursor = await connection.execute(sql.SQL("SELECT count(*) FROM {}.records").format(sql.Identifier(schema_name)))
        row = await cursor.fetchone()
        assert row is not None
        return int(row[0])
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_successful_delivery_commits_before_ack(postgres_boundary: tuple[str, str]) -> None:
    database_url, schema_name = postgres_boundary
    delivery = StrictDelivery()

    async def insert_record(connection: psycopg.AsyncConnection[Any], record: dict[str, Any]) -> None:
        await connection.execute("INSERT INTO records (id, name) VALUES (%s, %s)", (record["id"], record["name"]))

    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "connection_pool", RealConnectionPool(database_url, schema_name)),
        patch.dict(service.PROCESSORS, {"artists": insert_record}),
        patch.object(service, "message_counts", {"artists": 0}),
        patch.object(service, "last_message_time", {"artists": 0.0}),
    ):
        result = await service.on_data_message(delivery, "artists")  # type: ignore[arg-type]

    assert result == DeliveryResult(Settlement.ACK, "processed")
    assert delivery.calls == [("ack", None)]
    assert await _row_count(database_url, schema_name) == 1


@pytest.mark.asyncio
async def test_constraint_failure_rolls_back_and_rejects(postgres_boundary: tuple[str, str]) -> None:
    database_url, schema_name = postgres_boundary
    delivery = StrictDelivery()

    async def violate_constraint(connection: psycopg.AsyncConnection[Any], record: dict[str, Any]) -> None:
        await connection.execute("INSERT INTO records (id, name) VALUES (%s, NULL)", (record["id"],))

    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "connection_pool", RealConnectionPool(database_url, schema_name)),
        patch.dict(service.PROCESSORS, {"artists": violate_constraint}),
    ):
        result = await service.on_data_message(delivery, "artists")  # type: ignore[arg-type]

    assert result.settlement is Settlement.REJECT
    assert result.outcome == "deterministic"
    assert result.error_type == "NotNullViolation"
    assert delivery.calls == [("nack", False)]
    assert await _row_count(database_url, schema_name) == 0


@pytest.mark.asyncio
async def test_connection_loss_requeues_after_the_outage_wait(postgres_boundary: tuple[str, str]) -> None:
    database_url, schema_name = postgres_boundary
    delivery = StrictDelivery()
    waited = AsyncMock()

    async def lose_connection(connection: psycopg.AsyncConnection[Any], _record: dict[str, Any]) -> None:
        await connection.close()
        await connection.execute("SELECT 1")

    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "connection_pool", RealConnectionPool(database_url, schema_name)),
        patch.dict(service.PROCESSORS, {"artists": lose_connection}),
        patch.object(service.outage_backoff, "wait", waited),
    ):
        result = await service.on_data_message(delivery, "artists")  # type: ignore[arg-type]

    assert result.settlement is Settlement.REQUEUE
    assert result.outcome == "transient"
    assert result.error_type in {"InterfaceError", "OperationalError"}
    assert delivery.calls == [("nack", True)]
    waited.assert_awaited_once_with()
