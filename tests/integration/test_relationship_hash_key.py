"""A rerun of the same relationships creates no duplicates, with the loader naming the hashed key.

The loader's conflict target is the schema's `relationships_key` index (`musicbrainz.relationship_key`
over the eight values of the old natural key). These tests write through the loader's own
`insert_relationships` on a real PostgreSQL carrying the promoted schema, and the last one drops the
old eight-column constraint, which the schema does once MusicBrainz is about to be enabled, to show the
loader no longer depends on it.
"""

from __future__ import annotations

import os
import uuid
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool
from groovemap_schema.postgres import create_postgres_schema
from psycopg.conninfo import conninfo_to_dict

from brainztableinator._persistence import PostgreSQLMusicBrainzWriter
from brainztableinator._record_processing import MusicBrainzRecordProcessor


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

SOURCE = str(uuid.UUID(int=0x5001))
TARGET = str(uuid.UUID(int=0x7001))
OTHER_TARGET = str(uuid.UUID(int=0x7002))


class _NullObserver:
    def flush(self, entity: str) -> Any:
        del entity
        return nullcontext(None)

    def record(self, entity: str, size: int, duration_s: float, outcome: str) -> None:
        del entity, size, duration_s, outcome

    def set_outcome(self, span: Any, outcome: str) -> None:
        del span, outcome


def _relationship(target: str = TARGET, **fields: Any) -> dict[str, Any]:
    return {"target_mbid": target, "target_type": "artist", "type": "member of band", "attributes": [], **fields}


@pytest_asyncio.fixture
async def pool() -> AsyncIterator[AsyncPostgreSQLPool]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    built = AsyncPostgreSQLPool(connection_params=conninfo_to_dict(database_url), max_retries=1)
    await built.initialize()
    assert await create_postgres_schema(built) == 0
    async with built.connection() as conn:
        await conn.set_autocommit(True)
        await conn.execute("TRUNCATE musicbrainz.relationships")
    try:
        yield built
    finally:
        async with built.connection() as conn:
            await conn.set_autocommit(True)
            await conn.execute("TRUNCATE musicbrainz.relationships")
        assert await create_postgres_schema(built) == 0  # restores any constraint a test dropped
        await built.close()


async def _write(pool: AsyncPostgreSQLPool, relationships: list[dict[str, Any]]) -> None:
    writer = PostgreSQLMusicBrainzWriter()
    writer.set_refresh_updated_at(True)
    processor = MusicBrainzRecordProcessor(writer, _NullObserver(), lambda record: record)
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, SOURCE, "artist", relationships)


async def _rows(pool: AsyncPostgreSQLPool) -> list[tuple[Any, ...]]:
    async with pool.connection() as conn:
        cursor = await conn.execute(
            "SELECT target_mbid::text, relationship_type, begin_date, end_date, attributes::text, ended "
            "FROM musicbrainz.relationships ORDER BY target_mbid, begin_date NULLS FIRST, attributes::text"
        )
        return [tuple(row) for row in await cursor.fetchall()]


BATCH = [
    _relationship(begin_date="2000-01-01", end_date="2005-01-01", attributes=["guitar"], ended=True),
    _relationship(begin_date="2010-01-01", end_date=None, attributes=["guitar"]),
    _relationship(begin_date="2010-01-01", end_date=None, attributes=["guitar", "vocals"]),
    _relationship(),
    _relationship(OTHER_TARGET),
]


async def test_a_rerun_of_the_same_data_creates_no_duplicates(pool: AsyncPostgreSQLPool) -> None:
    await _write(pool, BATCH)
    first = await _rows(pool)
    assert len(first) == 5, "dates and attributes keep separate relationships apart; two NULL dates are one key"

    for _ in range(3):
        await _write(pool, BATCH)

    print(f"\nRERUN: {len(first)} rows after the first write, {len(await _rows(pool))} after three reruns of the same data")
    assert await _rows(pool) == first


async def test_a_rerun_refreshes_updated_at_and_changes_only_ended(pool: AsyncPostgreSQLPool) -> None:
    await _write(pool, [_relationship(begin_date="2000-01-01", ended=False)])
    async with pool.connection() as conn:
        await conn.execute("UPDATE musicbrainz.relationships SET updated_at = NOW() - INTERVAL '1 day'")
        stale = await (await conn.execute("SELECT updated_at FROM musicbrainz.relationships")).fetchone()

    await _write(pool, [_relationship(begin_date="2000-01-01", ended=True)])

    async with pool.connection() as conn:
        refreshed = await (await conn.execute("SELECT updated_at, ended FROM musicbrainz.relationships")).fetchall()
    assert len(refreshed) == 1
    assert stale is not None
    assert refreshed[0][0] > stale[0]
    assert refreshed[0][1] is True


async def test_the_loader_no_longer_depends_on_the_old_eight_column_constraint(pool: AsyncPostgreSQLPool) -> None:
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        await conn.execute("ALTER TABLE musicbrainz.relationships DROP CONSTRAINT relationships_natural_key")
    await _write(pool, BATCH)
    first = await _rows(pool)

    await _write(pool, BATCH)

    print(f"\nHASH KEY ONLY: constraint dropped, {len(first)} rows, {len(await _rows(pool))} after a rerun")
    assert len(first) == 5
    assert await _rows(pool) == first
