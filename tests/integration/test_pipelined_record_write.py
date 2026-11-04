"""Real-PostgreSQL regressions for the one-pipeline-per-record write path.

A record's upsert, media, relationship and link statements are queued on one connection
pipeline and settle together. A mock cannot show what matters here: that every row lands, and
that a failure in a *later* statement of the pipeline still rolls back the earlier ones,
because they all share the delivery's transaction.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import psycopg
import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool
from common.media import map_musicbrainz_release
from groovemap_schema.postgres import create_postgres_schema
from psycopg.conninfo import conninfo_to_dict

from brainztableinator._persistence import PostgreSQLMusicBrainzWriter
from brainztableinator._record_processing import MusicBrainzRecordProcessor


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

MBID = "5b1c7e0a-2f4d-4a6b-8c3e-9d0f1a2b3c4d"
TARGET = "6c2d8f1b-3a5e-4b7c-9d4f-0e1a2b3c4d5e"
DISCOGS_RELEASE_ID = "910000001"


class _Observer:
    def flush(self, _entity: str) -> Any:
        return MagicMock()

    def record(self, *_args: Any) -> None:
        return None

    def set_outcome(self, *_args: Any) -> None:
        return None


@pytest_asyncio.fixture
async def connection() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    pool = AsyncPostgreSQLPool(connection_params=conninfo_to_dict(database_url), max_retries=1)
    await pool.initialize()
    try:
        assert await create_postgres_schema(pool) == 0
    finally:
        await pool.close()
    conn = await psycopg.AsyncConnection.connect(database_url)
    await conn.set_autocommit(False)
    # Open the outer transaction now, so each `transaction()` block below is a savepoint and
    # the rollback in `finally` discards everything: an outermost block would commit.
    await conn.execute("SELECT 1")
    try:
        yield conn
    finally:
        await conn.rollback()
        await conn.close()


def _record(relations: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "mbid": MBID,
        "name": "A Release",
        "status": "Official",
        "barcode": "0123456789012",
        "discogs_release_id": DISCOGS_RELEASE_ID,
        "media": map_musicbrainz_release({"media": [{"format": "CD", "disc-count": 1}], "status": "Official"}),
        "relations": relations,
        "external_links": [{"url": "https://example.org/a", "service": "wikidata"}],
    }


def _processor() -> MusicBrainzRecordProcessor:
    return MusicBrainzRecordProcessor(PostgreSQLMusicBrainzWriter(), _Observer(), map_musicbrainz_release)


async def _count(conn: psycopg.AsyncConnection[Any], query: str, *params: Any) -> int:
    async with conn.cursor() as cursor:
        await cursor.execute(query, params)
        row = await cursor.fetchone()
    assert row is not None
    return int(row[0])


async def test_a_pipelined_release_writes_every_row_and_is_idempotent(connection: psycopg.AsyncConnection[Any]) -> None:
    relations = [{"target_mbid": TARGET, "target_type": "artist", "type": "producer"}]
    for _ in range(2):  # the second pass is a redelivery: same rows, no duplicates
        async with connection.transaction():
            await _processor().process_release(connection, _record(relations))

    assert await _count(connection, "SELECT count(*) FROM musicbrainz.releases WHERE mbid = %s", MBID) == 1
    assert await _count(connection, "SELECT count(*) FROM musicbrainz.relationships WHERE source_mbid = %s", MBID) == 1
    assert await _count(connection, "SELECT count(*) FROM musicbrainz.external_links WHERE mbid = %s", MBID) == 1
    assert await _count(connection, "SELECT count(*) FROM graph.issued_on WHERE release_id = %s AND source = 'musicbrainz'", DISCOGS_RELEASE_ID) == 1


async def test_a_failing_statement_late_in_the_pipeline_rolls_back_the_whole_record(connection: psycopg.AsyncConnection[Any]) -> None:
    # A target that is not a UUID fails the relationships statement, which is queued *after* the
    # artist upsert in the same pipeline, so the upsert has already been sent when it fails.
    artist = {"mbid": MBID, "name": "An Artist", "relations": [{"target_mbid": "not-a-uuid", "target_type": "label", "type": "member of"}]}
    with pytest.raises(psycopg.Error):
        async with connection.transaction():
            await _processor().process_artist(connection, artist)

    assert await _count(connection, "SELECT count(*) FROM musicbrainz.artists WHERE mbid = %s", MBID) == 0
    assert await _count(connection, "SELECT count(*) FROM musicbrainz.relationships WHERE source_mbid = %s OR target_mbid = %s", MBID, MBID) == 0


async def test_entity_data_omits_the_keys_that_have_their_own_tables(connection: psycopg.AsyncConnection[Any]) -> None:
    """`data` no longer repeats `relations` and `external_links`, and the tables still hold them."""
    record = _record([{"target_mbid": TARGET, "target_type": "artist", "type": "producer"}])
    async with connection.transaction():
        await _processor().process_release(connection, record)

    async with connection.cursor() as cursor:
        await cursor.execute("SELECT data ? 'relations', data ? 'external_links', data ->> 'name' FROM musicbrainz.releases WHERE mbid = %s", (MBID,))
        assert await cursor.fetchone() == (False, False, "A Release")
    assert await _count(connection, "SELECT count(*) FROM musicbrainz.relationships WHERE source_mbid = %s", MBID) == 1
    assert await _count(connection, "SELECT count(*) FROM musicbrainz.external_links WHERE mbid = %s", MBID) == 1
