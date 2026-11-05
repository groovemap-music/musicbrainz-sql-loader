"""Requeue pacing and the requeue budget on real RabbitMQ and real PostgreSQL.

The queue is declared like production: quorum, ``x-delivery-limit`` 20, with a dead-letter exchange.
PostgreSQL is reached through a TCP proxy this test can cut, so the pool sees a real outage.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import aio_pika
import psycopg
import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool, WriteAdmission
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

import brainztableinator.brainztableinator as service
from brainztableinator._reconciliation import StaleChildRowPurge


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.timeout(120)]


class TcpProxy:
    """A TCP proxy that can drop every connection and refuse new ones."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.listen_port = 0
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    async def open(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", self.listen_port)
        self.listen_port = self._server.sockets[0].getsockname()[1]

    async def cut(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
        for writer in list(self._writers):
            writer.transport.abort()
        self._writers.clear()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_reader, upstream_writer = await asyncio.open_connection(self.host, self.port)
        self._writers.update((writer, upstream_writer))
        await asyncio.gather(self._pipe(reader, upstream_writer), self._pipe(upstream_reader, writer))

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(OSError, RuntimeError):
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        writer.close()
        self._writers.discard(writer)


@dataclass
class Stack:
    proxy: TcpProxy
    dsn: str
    schema: str
    channel: aio_pika.abc.AbstractChannel
    queue: aio_pika.abc.AbstractQueue
    dlq: aio_pika.abc.AbstractQueue
    tag: str = ""
    boundary: datetime = field(default_factory=lambda: datetime.now(UTC))
    deliveries: list[float] = field(default_factory=list)

    async def publish(self, body: dict[str, Any]) -> None:
        message = aio_pika.Message(json.dumps(body).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await self.channel.default_exchange.publish(message, routing_key=self.queue.name)

    async def signal(self) -> None:
        await self.publish(
            {
                "type": "extraction_complete",
                "version": "2026-11-05",
                "timestamp": self.boundary.isoformat(),
                "started_at": self.boundary.isoformat(),
                "record_counts": {"artists": 1},
            }
        )

    async def rows(self) -> int:
        async with await psycopg.AsyncConnection.connect(self.dsn) as connection:
            cursor = await connection.execute(sql.SQL("SELECT count(*) FROM {}.purge_rows").format(sql.Identifier(self.schema)))
            row = await cursor.fetchone()
        assert row is not None
        return int(row[0])

    async def dead_letter_reason(self) -> str | None:
        deadline = asyncio.get_running_loop().time() + 3.0
        while (message := await self.dlq.get(fail=False, timeout=5)) is None:
            if asyncio.get_running_loop().time() >= deadline:
                return None
            await asyncio.sleep(0.05)
        await message.ack()
        reason = (message.headers or {}).get("x-first-death-reason")
        return reason if isinstance(reason, str) else None


async def until(predicate: Any, timeout: float) -> None:
    async with asyncio.timeout(timeout):
        while not await predicate():
            await asyncio.sleep(0.05)


@pytest_asyncio.fixture
async def stack() -> AsyncIterator[Stack]:
    """The real handler consuming a production-shaped queue, with PostgreSQL behind the proxy."""
    dsn = os.environ.get("TEST_DATABASE_URL")
    amqp_url = os.environ.get("TEST_RABBITMQ_URL")
    if not dsn or not amqp_url:
        pytest.skip("owned integration script must set TEST_DATABASE_URL and TEST_RABBITMQ_URL")

    schema = f"pacing_{uuid.uuid4().hex}"
    boundary = datetime.now(UTC)
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as admin:
        identifier = sql.Identifier(schema)
        await admin.execute(sql.SQL("CREATE SCHEMA {}").format(identifier))
        await admin.execute(sql.SQL("CREATE TABLE {}.purge_rows (id integer PRIMARY KEY, updated_at timestamptz NOT NULL)").format(identifier))
        await admin.execute(
            sql.SQL("INSERT INTO {}.purge_rows (id, updated_at) VALUES (%s, %s), (%s, %s), (%s, %s)").format(identifier),
            (1, boundary - timedelta(days=1), 2, boundary, 3, boundary),
        )

    params = conninfo_to_dict(dsn)
    proxy = TcpProxy(str(params["host"]), int(params["port"]))
    await proxy.open()
    pool = AsyncPostgreSQLPool(
        connection_params={
            "host": "127.0.0.1",
            "port": proxy.listen_port,
            "dbname": params["dbname"],
            "user": params["user"],
            "password": params["password"],
        },
        min_connections=1,
        max_connections=4,
        max_retries=1,
    )
    await pool.initialize()
    admission = WriteAdmission(2)
    purge = StaleChildRowPurge(pool, service.logger, tables=((schema, "purge_rows"),), expected_data_types=("artists",), admission=admission)

    connection = await aio_pika.connect(amqp_url)
    channel = await connection.channel(publisher_confirms=True)
    await channel.set_qos(prefetch_count=4, global_=False)
    name = f"gm-vd2q-{uuid.uuid4().hex[:8]}"
    exchange = await channel.declare_exchange(f"{name}.dlx", aio_pika.ExchangeType.FANOUT, durable=True)
    dlq = await channel.declare_queue(f"{name}.dlq", durable=True, arguments={"x-queue-type": "classic"})
    await dlq.bind(exchange)
    queue = await channel.declare_queue(
        name, durable=True, arguments={"x-queue-type": "quorum", "x-delivery-limit": 20, "x-dead-letter-exchange": f"{name}.dlx"}
    )
    built = Stack(proxy, dsn, schema, channel, queue, dlq, boundary=boundary)
    handler = service.make_data_handler("artists")
    loop = asyncio.get_running_loop()

    async def handle(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        built.deliveries.append(loop.time())
        await handler(message)

    service.outage_backoff.reset()
    with (
        patch.object(service, "connection_pool", pool),
        patch.object(service, "write_admission", admission),
        patch.object(service, "stale_row_purge", purge),
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "CONSUMER_CANCEL_DELAY", 0),
        patch.object(service, "completed_files", set()),
        patch.object(service, "consumer_cancel_tasks", {}),
    ):
        built.tag = await queue.consume(handle)
        try:
            yield built
        finally:
            for task in list(service.consumer_cancel_tasks.values()):
                task.cancel()
            with contextlib.suppress(Exception):
                await queue.cancel(built.tag)
            await proxy.cut()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(pool.close(), 5)
            await queue.delete(if_unused=False, if_empty=False)
            await dlq.delete(if_unused=False, if_empty=False)
            await exchange.delete()
            await connection.close()
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as admin:
                await admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


async def test_a_short_outage_loses_no_signal_and_the_deletes_reconcile(stack: Stack) -> None:
    """A failed reconciliation requeues at the backoff's pace, then the stale row goes."""
    await stack.proxy.cut()
    await stack.signal()
    await asyncio.sleep(10)
    during_outage = len(stack.deliveries)
    await stack.proxy.open()

    async def reconciled() -> bool:
        return await stack.rows() == 2

    await until(reconciled, 70)
    assert during_outage <= 4, f"signal redelivered {during_outage} times in a 10 s outage"
    assert await stack.dead_letter_reason() is None


async def test_an_unknown_exception_is_paced(stack: Stack) -> None:
    async def boom(_connection: Any, _data: dict[str, Any]) -> None:
        raise RuntimeError("not a database failure")

    with patch.dict(service.PROCESSORS, {"artists": boom}):
        await stack.publish({"id": str(uuid.uuid4()), "name": "poison"})
        await asyncio.sleep(7)
    assert 2 <= len(stack.deliveries) <= 4, f"record delivered {len(stack.deliveries)} times in 7 s"


async def test_a_persistent_failure_dead_letters_at_the_budget(stack: Stack) -> None:
    """The real broker's x-acquired-count ends it: exactly budget + 1 deliveries, then the DLQ."""
    await stack.proxy.cut()
    with patch.object(service, "MAX_REQUEUES", 2):
        await stack.signal()

        async def dead() -> bool:
            queue = await stack.channel.declare_queue(stack.dlq.name, passive=True)
            return queue.declaration_result.message_count == 1

        await until(dead, 60)
    assert len(stack.deliveries) == 3
    assert await stack.dead_letter_reason() == "rejected"
    assert await stack.rows() == 3


async def _dead_letter_the_signal(stack: Stack) -> bytes:
    """Run the signal through a persistent failure and hand back the body the DLQ holds."""
    await stack.proxy.cut()
    with patch.object(service, "MAX_REQUEUES", 1):
        await stack.signal()

        async def dead() -> bool:
            queue = await stack.channel.declare_queue(stack.dlq.name, passive=True)
            return queue.declaration_result.message_count == 1

        await until(dead, 60)
    message = await stack.dlq.get(fail=True, timeout=5)
    assert message is not None
    await message.ack()
    await stack.proxy.open()
    return message.body


async def test_replaying_a_dead_lettered_signal_recovers_the_run(stack: Stack) -> None:
    body = await _dead_letter_the_signal(stack)
    await stack.channel.default_exchange.publish(aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=stack.queue.name)

    async def reconciled() -> bool:
        return await stack.rows() == 2

    await until(reconciled, 70)


async def test_a_dead_lettered_signal_does_not_veto_the_next_clean_extraction(stack: Stack) -> None:
    await _dead_letter_the_signal(stack)
    stack.boundary += timedelta(hours=1)
    async with await psycopg.AsyncConnection.connect(stack.dsn) as connection:
        statement = sql.SQL("UPDATE {}.purge_rows SET updated_at = %s WHERE id > 1").format(sql.Identifier(stack.schema))
        await connection.execute(statement, (stack.boundary,))
    await stack.signal()

    async def reconciled() -> bool:
        return await stack.rows() == 2

    await until(reconciled, 70)


async def test_a_rejected_record_still_vetoes_the_run(stack: Stack) -> None:
    """A record at its budget never refreshed its row, so purging would delete a live record."""

    async def boom(_connection: Any, _data: dict[str, Any]) -> None:
        raise RuntimeError("not a database failure")

    with patch.dict(service.PROCESSORS, {"artists": boom}), patch.object(service, "MAX_REQUEUES", 0):
        await stack.publish({"id": str(uuid.uuid4()), "name": "poison"})
        assert await stack.dead_letter_reason() == "rejected"
    await stack.signal()

    async def settled() -> bool:
        return service.stale_row_purge.settled_boundary is not None

    await until(settled, 30)
    assert await stack.rows() == 3


async def test_the_grace_timer_cannot_strand_a_signal_in_backoff(stack: Stack) -> None:
    """file_complete arms the timer, the signal waits out a failure past it, the consumer stays."""
    await stack.proxy.cut()
    with (
        patch.object(service, "CONSUMER_CANCEL_DELAY", 3),
        patch.dict(service.queues, {"artists": stack.queue}),
        patch.dict(service.consumer_tags, {"artists": stack.tag}),
    ):
        await stack.publish({"type": "file_complete", "total_processed": 0})

        async def armed() -> bool:
            return "artists" in service.consumer_cancel_tasks

        await until(armed, 10)
        await stack.signal()
        await asyncio.sleep(8)
        assert "artists" in service.consumer_tags, "the grace timer cancelled the consumer while the signal was in backoff"
        await stack.proxy.open()

        async def reconciled() -> bool:
            return await stack.rows() == 2

        await until(reconciled, 70)
        assert "artists" in service.consumer_cancel_tasks, "landing the signal did not re-arm the grace timer"
