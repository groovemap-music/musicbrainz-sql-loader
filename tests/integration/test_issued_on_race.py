"""Real-PostgreSQL regression: concurrent releases that resolve to one native release.

Two MusicBrainz releases can name the same Discogs release, so their two messages write the same
`graph.issued_on` range `(release_id, source)` from two transactions. Each prunes nothing it can
see and inserts, and the second insert used to die on `issued_on_pkey` and dead-letter. These
tests use two committing connections, so the transactions genuinely overlap; unlike the other
integration tests they cannot roll back a single shared one, and they delete what they wrote.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool
from common.media import map_musicbrainz_release
from groovemap_schema.postgres import create_postgres_schema
from psycopg.conninfo import conninfo_to_dict

from brainztableinator._persistence import DuplicateMediumError, PostgreSQLMusicBrainzWriter
from brainztableinator._record_processing import MusicBrainzRecordProcessor


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

RELEASE_ID = "900000077"
MBID_FIRST = "11111111-2222-4333-8444-555555555501"
MBID_SECOND = "11111111-2222-4333-8444-555555555502"
# Hard bound on every wait: a timeout is a failure, never a hang.
DEADLINE_S = 20.0
HOLD_S = 1.0


class _Observer:
    def flush(self, _entity: str) -> Any:
        return MagicMock()

    def record(self, *_args: Any) -> None:
        return None

    def set_outcome(self, *_args: Any) -> None:
        return None


def _record(mbid: str, media: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "mbid": mbid,
        "name": "A Release",
        "status": "Official",
        "discogs_release_id": RELEASE_ID,
        "media": map_musicbrainz_release({"media": media, "status": "Official"}),
    }


def _processor() -> MusicBrainzRecordProcessor:
    return MusicBrainzRecordProcessor(PostgreSQLMusicBrainzWriter(), _Observer(), map_musicbrainz_release)


async def _wipe(url: str) -> None:
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as conn:
        await conn.execute("DELETE FROM graph.issued_on WHERE release_id = %s", (RELEASE_ID,))
        await conn.execute("DELETE FROM musicbrainz.releases WHERE mbid = ANY(%s::uuid[])", ([MBID_FIRST, MBID_SECOND],))


@pytest_asyncio.fixture
async def database_url() -> Any:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    pool = AsyncPostgreSQLPool(connection_params=conninfo_to_dict(url), max_retries=1)
    await pool.initialize()
    try:
        assert await create_postgres_schema(pool) == 0
    finally:
        await pool.close()
    await _wipe(url)
    yield url
    await _wipe(url)


async def _edges(url: str) -> list[tuple[str, int]]:
    async with await psycopg.AsyncConnection.connect(url) as conn, conn.cursor() as cursor:
        await cursor.execute(
            "SELECT medium_id, qty FROM graph.issued_on WHERE release_id = %s AND source = 'musicbrainz' ORDER BY medium_id", (RELEASE_ID,)
        )
        return [(row[0], int(row[1])) for row in await cursor.fetchall()]


async def test_two_concurrent_releases_for_one_native_release_both_commit(database_url: str) -> None:
    first = await psycopg.AsyncConnection.connect(database_url)
    second = await psycopg.AsyncConnection.connect(database_url)
    try:
        await first.set_autocommit(False)
        await second.set_autocommit(False)
        processor = _processor()

        # The first transaction has written its edges and not committed. The second starts now,
        # so it cannot see them, which is the interleaving the live load produced.
        await asyncio.wait_for(processor.process_release(first, _record(MBID_FIRST, [{"format": "CD"}])), DEADLINE_S)
        late = asyncio.create_task(processor.process_release(second, _record(MBID_SECOND, [{"format": "CD"}, {"format": "CD"}])))
        await asyncio.sleep(HOLD_S)
        await first.commit()

        await asyncio.wait_for(late, DEADLINE_S)
        await second.commit()
    finally:
        await first.close()
        await second.close()

    # Whichever commits last wins, and the edges are well defined: one row, the second's quantity.
    assert await _edges(database_url) == [("optical_cd", 2)]


async def test_a_duplicate_inside_one_records_rows_still_fails(database_url: str) -> None:
    async with await psycopg.AsyncConnection.connect(database_url) as conn:
        await conn.set_autocommit(False)
        rows = [(RELEASE_ID, "optical_cd", "musicbrainz", 1), (RELEASE_ID, "optical_cd", "musicbrainz", 2)]
        with pytest.raises(DuplicateMediumError, match="duplicate medium"):
            await PostgreSQLMusicBrainzWriter().replace_release_media_edges(conn, RELEASE_ID, "musicbrainz", rows)


async def _write(url: str, media: list[dict[str, Any]]) -> None:
    async with await psycopg.AsyncConnection.connect(url) as conn:
        await _processor().process_release(conn, _record(MBID_FIRST, media))
        await conn.commit()


async def _edge_versions(url: str) -> dict[str, str]:
    """Each stored edge's `xmin`: a row that was rewritten carries a new one."""
    async with await psycopg.AsyncConnection.connect(url) as conn, conn.cursor() as cursor:
        await cursor.execute("SELECT medium_id, xmin::text FROM graph.issued_on WHERE release_id = %s AND source = 'musicbrainz'", (RELEASE_ID,))
        return {row[0]: row[1] for row in await cursor.fetchall()}


async def test_an_unchanged_reload_rewrites_no_edge_and_a_changed_one_touches_only_the_difference(database_url: str) -> None:
    await _write(database_url, [{"format": "CD"}, {"format": '12" Vinyl'}])
    first = await _edge_versions(database_url)
    assert set(first) == {"optical_cd", "vinyl_12"}

    # Unchanged: every edge row keeps the transaction that wrote it, so nothing was rewritten.
    await _write(database_url, [{"format": "CD"}, {"format": '12" Vinyl'}])
    assert await _edge_versions(database_url) == first

    # Changed: the vinyl edge is removed, a second CD changes a quantity, a cassette is added.
    await _write(database_url, [{"format": "CD"}, {"format": "CD"}, {"format": "Cassette"}])
    after = await _edge_versions(database_url)
    assert await _edges(database_url) == [("optical_cd", 2), ("tape_cassette", 1)]
    assert "vinyl_12" not in after
    assert after["optical_cd"] != first["optical_cd"]

    # Unchanged again, and a release that lost every medium ends with no edge at all.
    await _write(database_url, [{"format": "CD"}, {"format": "CD"}, {"format": "Cassette"}])
    assert await _edge_versions(database_url) == after
    await _write(database_url, [])
    assert await _edges(database_url) == []
