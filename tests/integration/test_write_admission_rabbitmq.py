"""Owned RabbitMQ/PostgreSQL evidence for bounded consumers and recovery."""

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import aio_pika
import psycopg
import pytest
from common import WriteAdmission
from psycopg import sql

import brainztableinator.brainztableinator as service


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = pytest.mark.integration


class TrackedPostgresPool:
    """Minimal real pool protocol that records actual concurrent DB checkouts."""

    def __init__(self, dsn: str, schema: str) -> None:
        self._dsn = dsn
        self._schema = schema
        self.active = 0
        self.peak = 0

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        connection = await psycopg.AsyncConnection.connect(self._dsn)
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            yield connection
        finally:
            self.active -= 1
            await connection.close()


async def _count_rows(dsn: str, schema: str) -> int:
    async with await psycopg.AsyncConnection.connect(dsn) as connection:
        statement = sql.SQL("SELECT count(*) FROM {}.writes").format(sql.Identifier(schema))
        cursor = await connection.execute(statement)
        row = await cursor.fetchone()
        assert row is not None
        return int(row[0])


@pytest.mark.asyncio
async def test_real_consumers_bound_writes_and_recover_cancelled_delivery() -> None:
    dsn = os.environ.get("TEST_DATABASE_URL")
    amqp_url = os.environ.get("TEST_RABBITMQ_URL")
    if not dsn or not amqp_url:
        pytest.skip("owned integration script must set TEST_DATABASE_URL and TEST_RABBITMQ_URL")

    suffix = uuid.uuid4().hex
    schema = f"admission_{suffix}"
    admin = await psycopg.AsyncConnection.connect(dsn)
    await admin.set_autocommit(True)
    schema_ident = sql.Identifier(schema)
    await admin.execute(sql.SQL("CREATE SCHEMA {}").format(schema_ident))
    await admin.execute(sql.SQL("CREATE TABLE {}.writes (id uuid PRIMARY KEY, entity text NOT NULL)").format(schema_ident))
    await admin.close()

    pool = TrackedPostgresPool(dsn, schema)
    admission = WriteAdmission(2)
    connection = await aio_pika.connect(amqp_url)
    queues: dict[str, Any] = {}
    channels: list[Any] = []
    try:
        with (
            patch.object(service, "connection_pool", pool),
            patch.object(service, "write_admission", admission),
            patch.object(service, "shutdown_requested", False),
            patch.object(service, "message_counts", dict.fromkeys(service.MUSICBRAINZ_DATA_TYPES, 0)),
            patch.object(service, "last_message_time", dict.fromkeys(service.MUSICBRAINZ_DATA_TYPES, 0.0)),
        ):

            async def insert(connection: Any, data: dict[str, Any]) -> None:
                await asyncio.sleep(0.02)
                await connection.execute(
                    sql.SQL("INSERT INTO {}.writes (id, entity) VALUES (%s, %s)").format(sql.Identifier(schema)),
                    (data["id"], data["entity"]),
                )

            processors = dict.fromkeys(service.MUSICBRAINZ_DATA_TYPES, insert)
            with patch.dict(service.PROCESSORS, processors):
                channel = await connection.channel()
                channels.append(channel)
                await service._configure_consumer_qos(channel)
                for data_type in service.MUSICBRAINZ_DATA_TYPES:
                    exchange_name = service.catalog_exchange_name(data_type)
                    queue_name = service.catalog_queue_name(service.AMQP_CONSUMER_ID, data_type)
                    dlx_name = service.catalog_dead_letter_exchange_name(service.AMQP_CONSUMER_ID, data_type)
                    exchange = await channel.declare_exchange(exchange_name, service.AMQP_EXCHANGE_TYPE, durable=True)
                    dlx = await channel.declare_exchange(dlx_name, service.AMQP_EXCHANGE_TYPE, durable=True)
                    dlq = await channel.declare_queue(
                        service.catalog_dead_letter_queue_name(service.AMQP_CONSUMER_ID, data_type),
                        durable=True,
                        arguments={"x-queue-type": "classic"},
                    )
                    await dlq.bind(dlx)
                    queue = await channel.declare_queue(
                        queue_name,
                        durable=True,
                        arguments={
                            "x-queue-type": "quorum",
                            "x-dead-letter-exchange": dlx_name,
                            "x-delivery-limit": 20,
                        },
                    )
                    await queue.bind(exchange)
                    queues[data_type] = queue
                    await queue.consume(service.make_data_handler(data_type))

                for data_type in service.MUSICBRAINZ_DATA_TYPES:
                    for _ in range(4):
                        record_id = str(uuid.uuid4())
                        body = (f'{{"id":"{record_id}","name":"load","entity":"{data_type}"}}').encode()
                        await channel.default_exchange.publish(aio_pika.Message(body=body), routing_key=queues[data_type].name)
                for _ in range(100):
                    if await _count_rows(dsn, schema) == 16:
                        break
                    await asyncio.sleep(0.05)
                assert await _count_rows(dsn, schema) == 16
                assert pool.peak <= 2
                for _ in range(100):
                    if pool.active == 0:
                        break
                    await asyncio.sleep(0.01)
                assert pool.active == 0

                # A first delivery is canceled while it holds a DB slot. Closing its
                # channel returns the unacked message to the durable queue; a fresh
                # service recovery path then registers all four consumers and settles it.
                await channel.close()
                recovery_channel = await connection.channel()
                channels.append(recovery_channel)
                await service._configure_consumer_qos(recovery_channel)
                recovery_queue = await recovery_channel.declare_queue(
                    service.catalog_queue_name(service.AMQP_CONSUMER_ID, "artists"),
                    durable=True,
                    arguments={
                        "x-queue-type": "quorum",
                        "x-dead-letter-exchange": service.catalog_dead_letter_exchange_name(service.AMQP_CONSUMER_ID, "artists"),
                        "x-delivery-limit": 20,
                    },
                )
                recovery_id = str(uuid.uuid4())
                started = asyncio.Event()
                first_tasks: list[asyncio.Task[Any]] = []

                async def cancel_first(message: Any) -> None:
                    first_tasks.append(asyncio.current_task())  # type: ignore[arg-type]

                    async def block(_connection: Any, _data: dict[str, Any]) -> None:
                        started.set()
                        await asyncio.Future()

                    with patch.dict(service.PROCESSORS, {"artists": block}):
                        await service.on_data_message(message, "artists")

                first_consumer_tag = await recovery_queue.consume(cancel_first)
                await recovery_channel.default_exchange.publish(
                    aio_pika.Message(body=f'{{"id":"{recovery_id}","name":"recovery","entity":"artists"}}'.encode()),
                    routing_key=recovery_queue.name,
                )
                await asyncio.wait_for(started.wait(), timeout=5)
                assert first_tasks and first_tasks[0] is not None
                first_tasks[0].cancel()
                await recovery_queue.cancel(first_consumer_tag)
                await recovery_channel.close()
                with pytest.raises(asyncio.CancelledError):
                    await first_tasks[0]
                assert pool.active == 0
                assert await _count_rows(dsn, schema) == 16

                # Re-enter the loader's actual broker recovery path on the owned broker.
                with (
                    patch.object(service, "rabbitmq_manager", SimpleNamespace(connect=AsyncMock(return_value=connection))),
                    patch.object(service, "active_connection", None),
                    patch.object(service, "active_channel", None),
                    patch.object(service, "consumer_tags", {}),
                    patch.object(service, "connection_consumer_tags", {}),
                    patch.object(service, "queues", {}),
                    patch.object(service, "consumer_cancellation_failed", False),
                ):
                    await service._recover_consumers()
                    channels.append(service.active_channel)

                for _ in range(100):
                    if await _count_rows(dsn, schema) == 17:
                        break
                    await asyncio.sleep(0.05)
                assert await _count_rows(dsn, schema) == 17
                assert pool.peak <= 2
    finally:
        for channel in channels:
            if not channel.is_closed:
                await channel.close()
        await connection.close()
        cleanup = await psycopg.AsyncConnection.connect(dsn)
        await cleanup.set_autocommit(True)
        await cleanup.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        await cleanup.close()
