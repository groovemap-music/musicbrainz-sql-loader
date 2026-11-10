"""Real-PostgreSQL proof that a completed MusicBrainz extraction refreshes the derived graph relations."""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool
from groovemap_schema.postgres import create_postgres_schema
from psycopg.conninfo import conninfo_to_dict

import brainztableinator._derived_refresh as derived
from brainztableinator._derived_refresh import derived_refresh_available, refresh_derived_relations


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

LOGGER: Any = type("Logger", (), {"info": lambda *_, **__: None, "warning": lambda *_, **__: None, "error": lambda *_, **__: None})()
MEMBER, BAND = uuid.uuid4(), uuid.uuid4()


@pytest_asyncio.fixture
async def pool() -> AsyncIterator[AsyncPostgreSQLPool]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    pool = AsyncPostgreSQLPool(connection_params=conninfo_to_dict(database_url), max_retries=1)
    await pool.initialize()
    assert await create_postgres_schema(pool) == 0
    async with pool.connection() as conn:
        for statement in (
            "TRUNCATE public.loader_derived_refresh_job, public.loader_derived_refresh_cursor, public.loader_extraction_latch CASCADE",
            "TRUNCATE musicbrainz.relationships, musicbrainz.artists, graph.artist_member_of, graph.vertex_degree CASCADE",
        ):
            await conn.execute(statement)
    try:
        yield pool
    finally:
        await pool.close()


async def _scalar(pool: AsyncPostgreSQLPool, sql: str, *params: Any) -> Any:
    async with pool.connection() as conn:
        cursor = await conn.execute(sql, params)
        row = await cursor.fetchone()
        return row[0] if row else None


async def _membership(pool: AsyncPostgreSQLPool, *, present: bool) -> None:
    async with pool.connection() as conn:
        await conn.execute("TRUNCATE musicbrainz.relationships")
        await conn.execute("DELETE FROM musicbrainz.artists")
        for mbid, name, discogs in ((MEMBER, "member", 11), (BAND, "band", 22)):
            await conn.execute("INSERT INTO musicbrainz.artists (mbid, name, discogs_artist_id) VALUES (%s, %s, %s)", (mbid, name, discogs))
        if present:
            await conn.execute(
                "INSERT INTO musicbrainz.relationships (source_mbid, target_mbid, source_entity_type, target_entity_type, relationship_type) "
                "VALUES (%s, %s, 'artist', 'artist', 'member of band')",
                (MEMBER, BAND),
            )


async def _mb_members(pool: AsyncPostgreSQLPool) -> int:
    return int(await _scalar(pool, "SELECT count(*) FROM graph.artist_member_of WHERE source = 'musicbrainz'"))


async def test_the_schema_declares_what_the_refresh_needs(pool: AsyncPostgreSQLPool) -> None:
    assert await derived_refresh_available(pool)


async def test_a_completed_extraction_refreshes_member_of_and_degree_and_records_the_job(pool: AsyncPostgreSQLPool) -> None:
    await _membership(pool, present=True)
    assert await _mb_members(pool) == 0

    counts = await refresh_derived_relations(pool, LOGGER, "20261101")

    assert counts is not None
    assert counts["refresh_artist_member_of"] == {"discogs": 0, "musicbrainz": 1}
    assert await _mb_members(pool) == 1
    assert (
        await _scalar(pool, "SELECT state FROM public.loader_derived_refresh_job WHERE loader = 'musicbrainz' AND version = '20261101'")
        == "completed"
    )
    assert await _scalar(
        pool, "SELECT refreshed_at IS NOT NULL FROM public.loader_extraction_latch WHERE loader = 'musicbrainz' AND version = '20261101'"
    )
    assert await _scalar(pool, "SELECT degree FROM graph.vertex_degree WHERE kind = 'a' AND key = '11'") is not None


async def test_a_removed_membership_leaves_the_union_on_the_next_extraction(pool: AsyncPostgreSQLPool) -> None:
    await _membership(pool, present=True)
    await refresh_derived_relations(pool, LOGGER, "20261101")
    await _membership(pool, present=False)
    await refresh_derived_relations(pool, LOGGER, "20261201")
    assert await _mb_members(pool) == 0
    assert await _scalar(pool, "SELECT count(*) FROM public.loader_derived_refresh_job WHERE loader = 'musicbrainz' AND state = 'completed'") == 2


async def test_a_redelivered_signal_does_not_refresh_again(pool: AsyncPostgreSQLPool) -> None:
    await _membership(pool, present=True)
    assert await refresh_derived_relations(pool, LOGGER, "20261101") is not None
    assert await refresh_derived_relations(pool, LOGGER, "20261101") is None
    assert await _scalar(pool, "SELECT attempt_count FROM public.loader_derived_refresh_job WHERE loader = 'musicbrainz'") == 1


async def test_a_failed_refresh_is_a_retry_job_and_the_next_delivery_completes_it(pool: AsyncPostgreSQLPool, monkeypatch: pytest.MonkeyPatch) -> None:
    await _membership(pool, present=True)
    monkeypatch.setitem(derived._REFRESH_SQL, "refresh_vertex_degree", "SELECT * FROM graph.no_such_function()")
    with pytest.raises(Exception, match="no_such_function"):
        await refresh_derived_relations(pool, LOGGER, "20261101")
    # Rolled back as one: the union the first function rebuilt did not commit.
    assert await _mb_members(pool) == 0
    state, error = None, None
    async with pool.connection() as conn:
        cursor = await conn.execute("SELECT state, last_error FROM public.loader_derived_refresh_job WHERE loader = 'musicbrainz'")
        state, error = await cursor.fetchone()
    assert state == "retry"
    assert "no_such_function" in error
    assert await _scalar(pool, "SELECT refreshed_at IS NULL FROM public.loader_extraction_latch WHERE loader = 'musicbrainz'")

    monkeypatch.undo()
    assert await refresh_derived_relations(pool, LOGGER, "20261101") is not None
    assert await _mb_members(pool) == 1
    assert await _scalar(pool, "SELECT attempt_count FROM public.loader_derived_refresh_job WHERE loader = 'musicbrainz'") == 2


async def test_a_concurrent_discogs_refresh_serializes_with_this_one(pool: AsyncPostgreSQLPool) -> None:
    await _membership(pool, present=True)
    async with pool.connection() as discogs:
        await discogs.set_autocommit(False)
        async with discogs.transaction():
            # Discogs' pass holds the advisory lock until its transaction commits.
            await discogs.execute("SELECT * FROM graph.refresh_artist_member_of()")
            task = asyncio.create_task(refresh_derived_relations(pool, LOGGER, "20261101"))
            await asyncio.sleep(1.0)
            assert not task.done(), "the MusicBrainz refresh must queue behind the Discogs one"
        counts = await asyncio.wait_for(task, timeout=30)
    assert counts is not None
    assert await _mb_members(pool) == 1
