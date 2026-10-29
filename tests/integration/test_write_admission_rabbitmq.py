"""Owned RabbitMQ/PostgreSQL evidence for bounded consumers and recovery."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import aio_pika
import psycopg
import pytest
from common import AsyncPostgreSQLPool, WriteAdmission
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

import brainztableinator.brainztableinator as service
from brainztableinator._reconciliation import StaleChildRowPurge


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = pytest.mark.integration


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
    await admin.execute(
        sql.SQL("CREATE TABLE {}.writes (id uuid PRIMARY KEY, entity text NOT NULL, updated_at timestamptz NOT NULL DEFAULT NOW())").format(
            schema_ident
        )
    )
    await admin.execute(sql.SQL("CREATE TABLE {}.purge_rows (id integer PRIMARY KEY, updated_at timestamptz NOT NULL)").format(schema_ident))
    await admin.close()

    params = conninfo_to_dict(dsn)
    pool = AsyncPostgreSQLPool(
        connection_params={key: params[key] for key in ("host", "port", "dbname", "user", "password")},
        min_connections=1,
        max_connections=4,
        max_retries=1,
    )
    await pool.initialize()
    pool.active = 0
    pool.peak = 0
    pool.checkout_count = 0
    original_connection = pool.connection

    @asynccontextmanager
    async def tracked_connection() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        async with original_connection() as pooled_connection:
            pool.active += 1
            pool.checkout_count += 1
            pool.peak = max(pool.peak, pool.active)
            try:
                yield pooled_connection
            finally:
                pool.active -= 1

    pool.connection = tracked_connection
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
                recovered_handler_started = asyncio.Event()
                recovered_handler_settled = asyncio.Event()
                recovered_slot_attempted = asyncio.Event()
                first_tasks: list[asyncio.Task[Any]] = []
                original_on_data_message = service.on_data_message

                async def capture_handler_task(message: Any, data_type: str) -> Any:
                    task = asyncio.current_task()
                    if task is not None and data_type == "artists":
                        first_tasks.append(task)
                        if len(first_tasks) == 2:
                            recovered_handler_started.set()
                    result = await original_on_data_message(message, data_type)
                    if data_type == "artists" and len(first_tasks) == 2:
                        recovered_handler_settled.set()
                    return result

                async def block(_connection: Any, _data: dict[str, Any]) -> None:
                    started.set()
                    await asyncio.Future()

                with patch.object(service, "on_data_message", capture_handler_task), patch.dict(service.PROCESSORS, {"artists": block}):
                    first_consumer_tag = await recovery_queue.consume(service.make_data_handler("artists"))
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

                # Occupy the complete old budget on real pooled connections while
                # the broker channel is replaced. The requeued callback must enter
                # the new consumer but cannot check out a connection until these
                # old-lifespan SQL scopes release their slots.
                release_old_holders = asyncio.Event()
                old_holders_started: asyncio.Queue[None] = asyncio.Queue()
                original_recovery_slot = admission.slot

                @asynccontextmanager
                async def observe_recovery_slot() -> AsyncIterator[None]:
                    recovered_slot_attempted.set()
                    async with original_recovery_slot():
                        yield

                async def hold_old_sql_scope() -> None:
                    async with original_recovery_slot(), pool.connection():
                        old_holders_started.put_nowait(None)
                        await release_old_holders.wait()

                # Re-enter the loader's actual broker recovery path on the owned broker.
                old_holders: list[asyncio.Task[None]] = []
                try:
                    for _ in range(2):
                        old_holders.append(asyncio.create_task(hold_old_sql_scope()))
                    await asyncio.wait_for(old_holders_started.get(), timeout=5)
                    await asyncio.wait_for(old_holders_started.get(), timeout=5)
                    assert pool.active == 2
                    assert await _count_rows(dsn, schema) == 16

                    admission.slot = observe_recovery_slot
                    with (
                        patch.object(service, "rabbitmq_manager", SimpleNamespace(connect=AsyncMock(return_value=connection))),
                        patch.object(service, "active_connection", None),
                        patch.object(service, "active_channel", None),
                        patch.object(service, "consumer_tags", {}),
                        patch.object(service, "connection_consumer_tags", {}),
                        patch.object(service, "queues", {}),
                        patch.object(service, "consumer_cancellation_failed", False),
                        patch.object(service, "on_data_message", capture_handler_task),
                    ):
                        await service._recover_consumers()
                        broker_channel = service.active_channel
                        channels.append(broker_channel)

                    await asyncio.wait_for(recovered_handler_started.wait(), timeout=5)
                    await asyncio.wait_for(recovered_slot_attempted.wait(), timeout=5)
                    assert service.write_admission is admission
                    assert pool.active == 2
                    assert await _count_rows(dsn, schema) == 16
                    assert await admission.drain(timeout=0) is False

                    release_old_holders.set()
                    await asyncio.gather(*old_holders)
                    await asyncio.wait_for(recovered_handler_settled.wait(), timeout=5)
                    assert await _count_rows(dsn, schema) == 17
                    assert pool.peak <= 2
                finally:
                    release_old_holders.set()
                    for task in old_holders:
                        task.cancel()
                    await asyncio.gather(*old_holders, return_exceptions=True)
                    admission.slot = original_recovery_slot

                # Real extraction_complete callbacks share the same admission.
                # Hold every permit so the final completion cannot start the SQL
                # purge or settle its boundary until both previous writers finish.
                purge_started_at = datetime.now(UTC)
                await admin_insert_purge_rows(dsn, schema, purge_started_at)
                purge = StaleChildRowPurge(pool, service.logger, tables=((schema, "purge_rows"),), admission=admission)
                with (
                    patch.object(service, "stale_row_purge", purge),
                    patch.object(service, "completed_files", set()),
                    patch.object(service, "CONSUMER_CANCEL_DELAY", 0),
                ):
                    release_purge_holders = asyncio.Event()
                    purge_holders_started: asyncio.Queue[None] = asyncio.Queue()

                    original_slot = admission.slot

                    async def hold_purge_permit() -> None:
                        async with original_slot(), pool.connection():
                            purge_holders_started.put_nowait(None)
                            await release_purge_holders.wait()

                    purge_holders = [asyncio.create_task(hold_purge_permit()) for _ in range(2)]
                    purge_waiting = asyncio.Event()

                    @asynccontextmanager
                    async def observe_purge_slot() -> AsyncIterator[None]:
                        purge_waiting.set()
                        async with original_slot():
                            yield

                    admission.slot = observe_purge_slot
                    try:
                        await asyncio.wait_for(purge_holders_started.get(), timeout=5)
                        await asyncio.wait_for(purge_holders_started.get(), timeout=5)
                        assert pool.active == 2

                        completion_results: list[tuple[str, Any]] = []
                        completion_results_ready = asyncio.Event()
                        three_completions_settled = asyncio.Event()
                        original_on_data_message_for_completion = service.on_data_message

                        async def capture_completion_result(message: Any, data_type: str) -> Any:
                            result = await original_on_data_message_for_completion(message, data_type)
                            if message.body.find(b"extraction_complete") >= 0:
                                completion_results.append((data_type, result))
                                if len(completion_results) == len(service.MUSICBRAINZ_DATA_TYPES) - 1:
                                    three_completions_settled.set()
                                if len(completion_results) == len(service.MUSICBRAINZ_DATA_TYPES):
                                    completion_results_ready.set()
                            return result

                        with patch.object(service, "on_data_message", capture_completion_result):
                            for data_type in service.MUSICBRAINZ_DATA_TYPES:
                                signal = {
                                    "type": "extraction_complete",
                                    "version": "2026-09-18",
                                    "timestamp": purge_started_at.isoformat(),
                                    "started_at": purge_started_at.isoformat(),
                                    "record_counts": dict.fromkeys(service.MUSICBRAINZ_DATA_TYPES, 1),
                                }
                                await broker_channel.default_exchange.publish(
                                    aio_pika.Message(body=json.dumps(signal).encode()),
                                    routing_key=service.catalog_queue_name(service.AMQP_CONSUMER_ID, data_type),
                                )

                            await asyncio.wait_for(purge_waiting.wait(), timeout=5)
                            assert purge.is_latched()
                            assert purge.settled_boundary is None
                            await asyncio.wait_for(three_completions_settled.wait(), timeout=5)
                            assert len(completion_results) == len(service.MUSICBRAINZ_DATA_TYPES) - 1
                            assert await _count_purge_rows(dsn, schema) == 3
                            assert pool.active == 2

                            release_purge_holders.set()
                            await asyncio.gather(*purge_holders)
                            await asyncio.wait_for(completion_results_ready.wait(), timeout=5)

                        assert purge.settled_boundary == purge_started_at
                        assert await _count_purge_rows(dsn, schema) == 2
                        assert len(completion_results) == len(service.MUSICBRAINZ_DATA_TYPES)
                        assert all(result.settlement is service.Settlement.ACK for _, result in completion_results)
                        assert all(result.outcome == "skipped" for _, result in completion_results)
                        checkouts_after_purge = pool.checkout_count

                        # A duplicate completion at the settled boundary is ACKed
                        # without reopening the purge or deleting another row.
                        duplicate_done = asyncio.Event()
                        duplicate_results: list[Any] = []

                        async def capture_duplicate(message: Any, data_type: str) -> Any:
                            result = await original_on_data_message_for_completion(message, data_type)
                            if message.body.find(b"extraction_complete") >= 0:
                                duplicate_results.append(result)
                                duplicate_done.set()
                            return result

                        with patch.object(service, "on_data_message", capture_duplicate):
                            duplicate_signal = {
                                "type": "extraction_complete",
                                "version": "2026-09-18",
                                "timestamp": purge_started_at.isoformat(),
                                "started_at": purge_started_at.isoformat(),
                                "record_counts": dict.fromkeys(service.MUSICBRAINZ_DATA_TYPES, 1),
                            }
                            await broker_channel.default_exchange.publish(
                                aio_pika.Message(body=json.dumps(duplicate_signal).encode()),
                                routing_key=service.catalog_queue_name(service.AMQP_CONSUMER_ID, "artists"),
                            )
                            await asyncio.wait_for(duplicate_done.wait(), timeout=5)
                        assert await _count_purge_rows(dsn, schema) == 2
                        assert purge.settled_boundary == purge_started_at
                        assert len(duplicate_results) == 1
                        assert duplicate_results[0].settlement is service.Settlement.ACK
                        assert pool.checkout_count == checkouts_after_purge
                    finally:
                        release_purge_holders.set()
                        await asyncio.gather(*purge_holders, return_exceptions=True)
                        admission.slot = original_slot
    finally:
        for channel in channels:
            if not channel.is_closed:
                await channel.close()
        await connection.close()
        pool.connection = original_connection
        await pool.close()
        cleanup = await psycopg.AsyncConnection.connect(dsn)
        await cleanup.set_autocommit(True)
        await cleanup.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        await cleanup.close()


async def admin_insert_purge_rows(dsn: str, schema: str, boundary: datetime) -> None:
    """Create one stale and two fresh rows for the owned real-purge assertion."""
    async with await psycopg.AsyncConnection.connect(dsn) as connection:
        statement = sql.SQL("INSERT INTO {}.purge_rows (id, updated_at) VALUES (%s, %s), (%s, %s), (%s, %s)").format(sql.Identifier(schema))
        await connection.execute(statement, (1, boundary - timedelta(days=1), 2, boundary, 3, boundary))


async def _count_purge_rows(dsn: str, schema: str) -> int:
    async with await psycopg.AsyncConnection.connect(dsn) as connection:
        statement = sql.SQL("SELECT count(*) FROM {}.purge_rows").format(sql.Identifier(schema))
        cursor = await connection.execute(statement)
        row = await cursor.fetchone()
        assert row is not None
        return int(row[0])
