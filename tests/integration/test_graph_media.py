"""Real-PostgreSQL regressions for the MusicBrainz half of graph.issued_on.

`graph.issued_on`, `graph.medium`, and `graph.media_family` are the three relations the
persistence contract records as owned by `discogs-sql-loader` *and* `musicbrainz-sql-loader`.
What has to hold against a real database is therefore not only that this loader writes its own
rows, but that writing them leaves the other loader's rows exactly as they were — which is what
the `source` column in the primary key `(release_id, medium_id, source)` exists for, and what
these tests assert by counting the table per source before and after.

Every test runs inside one transaction that is rolled back, so the shared integration database
is left as it was found.
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
    from contextlib import AbstractContextManager

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# One Discogs release the two loaders both have something to say about, and the MusicBrainz
# release that names it. Both are confined to the rolled-back transaction.
RELEASE_ID = "900000001"
RELEASE_MBID = "3f2b9c4e-6a1d-4f57-9a9b-2c3d4e5f6071"
UNMATCHED_MBID = "7c1a2b3d-4e5f-4061-8172-93a4b5c6d7e8"

# What `discogs-sql-loader` has already written for that release. Nothing this loader does may
# change a single one of these rows.
DISCOGS_EDGES = (
    (RELEASE_ID, "optical_cd", "discogs", 1),
    (RELEASE_ID, "vinyl_12", "discogs", 2),
)


class _Observer:
    """The batch-telemetry seam, reduced to what the record processor calls on it."""

    def flush(self, _entity: str) -> AbstractContextManager[Any]:
        return MagicMock()

    def record(self, _entity: str, _size: int, _duration_s: float, _outcome: str) -> None:
        return None

    def set_outcome(self, _span: Any, _outcome: str) -> None:
        return None


def _release(media: list[dict[str, Any]], *, discogs_release_id: str | None = RELEASE_ID, mbid: str = RELEASE_MBID) -> dict[str, Any]:
    """Return a releases event carrying the canonical block its producer computed."""
    return {
        "mbid": mbid,
        "name": "A Release",
        "status": "Official",
        "discogs_release_id": discogs_release_id,
        "media": map_musicbrainz_release({"media": media, "status": "Official"}),
    }


@pytest_asyncio.fixture
async def graph_connection() -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    """Apply the pinned schema, then hand out one connection whose work is always rolled back."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")

    connection_params: dict[str, Any] = conninfo_to_dict(database_url)
    pool = AsyncPostgreSQLPool(connection_params=connection_params, max_retries=1)
    await pool.initialize()
    try:
        failures = await create_postgres_schema(pool)
        assert failures == 0, f"{failures} schema statements failed against the integration container"
    finally:
        await pool.close()

    connection = await psycopg.AsyncConnection.connect(database_url)
    await connection.set_autocommit(False)
    try:
        yield connection
    finally:
        await connection.rollback()
        await connection.close()


async def _seed_discogs_rows(conn: psycopg.AsyncConnection[Any]) -> None:
    """Write the rows the other owner of these three relations has already written."""
    async with conn.cursor() as cursor:
        await cursor.executemany(
            "INSERT INTO graph.issued_on (release_id, medium_id, source, qty) VALUES (%s, %s, %s, %s)",
            DISCOGS_EDGES,
        )
        # A medium vertex the Discogs loader created, deliberately carrying values this
        # loader's rows disagree with, so an upsert that overwrote would be visible.
        await cursor.execute(
            "INSERT INTO graph.medium (medium_id, family, label) VALUES ('optical_cd', 'discogs_family', 'Discogs label')",
        )
        await cursor.execute("INSERT INTO graph.media_family (name) VALUES ('discogs_family')")


async def _counts_by_source(conn: psycopg.AsyncConnection[Any]) -> dict[str, tuple[int, int]]:
    """Return the row count and total quantity this release carries, per source."""
    async with conn.cursor() as cursor:
        await cursor.execute(
            "SELECT source, count(*), COALESCE(sum(qty), 0) FROM graph.issued_on WHERE release_id = %s GROUP BY source ORDER BY source",
            (RELEASE_ID,),
        )
        rows = await cursor.fetchall()
    return {row[0]: (int(row[1]), int(row[2])) for row in rows}


async def _edges(conn: psycopg.AsyncConnection[Any], source: str) -> list[tuple[str, int]]:
    async with conn.cursor() as cursor:
        await cursor.execute(
            "SELECT medium_id, qty FROM graph.issued_on WHERE release_id = %s AND source = %s ORDER BY medium_id",
            (RELEASE_ID, source),
        )
        return [(row[0], int(row[1])) for row in await cursor.fetchall()]


def _processor() -> MusicBrainzRecordProcessor:
    return MusicBrainzRecordProcessor(PostgreSQLMusicBrainzWriter(), _Observer(), map_musicbrainz_release)


async def test_a_release_writes_its_own_edges_and_leaves_the_other_source_alone(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    await _seed_discogs_rows(graph_connection)

    await _processor().process_release(graph_connection, _release([{"format": '12" Vinyl'}, {"format": '12" Vinyl'}, {"format": "CD"}]))

    # Per source: two rows each, and the quantities the two loaders each derived. The Discogs
    # pair is byte-for-byte what was seeded.
    assert await _counts_by_source(graph_connection) == {"discogs": (2, 3), "musicbrainz": (2, 3)}
    assert await _edges(graph_connection, "discogs") == [("optical_cd", 1), ("vinyl_12", 2)]
    assert await _edges(graph_connection, "musicbrainz") == [("optical_cd", 1), ("vinyl_12", 2)]


async def test_reprocessing_a_release_prunes_only_its_own_source(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    await _seed_discogs_rows(graph_connection)
    processor = _processor()

    await processor.process_release(graph_connection, _release([{"format": '12" Vinyl'}, {"format": '12" Vinyl'}, {"format": "CD"}]))
    # The release is re-stated as a single CD: the vinyl edge this loader wrote must go, and
    # the vinyl edge the Discogs loader wrote must stay.
    await processor.process_release(graph_connection, _release([{"format": "CD"}]))

    assert await _counts_by_source(graph_connection) == {"discogs": (2, 3), "musicbrainz": (1, 1)}
    assert await _edges(graph_connection, "discogs") == [("optical_cd", 1), ("vinyl_12", 2)]
    assert await _edges(graph_connection, "musicbrainz") == [("optical_cd", 1)]


async def test_a_release_whose_media_no_longer_names_a_medium_keeps_no_stale_edge(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    await _seed_discogs_rows(graph_connection)
    processor = _processor()

    await processor.process_release(graph_connection, _release([{"format": "CD"}]))
    await processor.process_release(graph_connection, _release([]))

    assert await _counts_by_source(graph_connection) == {"discogs": (2, 3)}


async def test_a_release_naming_no_discogs_id_writes_no_edge(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    # The graph keys a release on its Discogs id, so this record has no key to write a row
    # under. brainzgraphinator counts the same record under `entities_skipped_no_discogs_match`
    # and reconciles no media. The release document itself is still stored.
    await _seed_discogs_rows(graph_connection)

    record = _release([{"format": "CD"}], discogs_release_id=None, mbid=UNMATCHED_MBID)
    await _processor().process_release(graph_connection, record)

    assert await _counts_by_source(graph_connection) == {"discogs": (2, 3)}
    async with graph_connection.cursor() as cursor:
        await cursor.execute("SELECT count(*) FROM graph.issued_on WHERE source = 'musicbrainz'")
        assert (await cursor.fetchone())[0] == 0
        await cursor.execute("SELECT count(*) FROM musicbrainz.releases WHERE mbid = %s", (UNMATCHED_MBID,))
        assert (await cursor.fetchone())[0] == 1


async def test_the_shared_vocabulary_gains_the_new_rows_and_keeps_the_existing_ones(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    await _seed_discogs_rows(graph_connection)

    await _processor().process_release(graph_connection, _release([{"format": '12" Vinyl'}, {"format": "CD"}]))

    async with graph_connection.cursor() as cursor:
        await cursor.execute("SELECT medium_id, family, label FROM graph.medium WHERE medium_id IN ('optical_cd', 'vinyl_12') ORDER BY medium_id")
        media = await cursor.fetchall()
        await cursor.execute("SELECT name FROM graph.media_family WHERE name IN ('discogs_family', 'optical', 'vinyl') ORDER BY name")
        families = [row[0] for row in await cursor.fetchall()]

    # `optical_cd` keeps what the Discogs loader wrote — the enricher sets a medium's family
    # and label ON CREATE only — while `vinyl_12` is created with this loader's values.
    assert media == [("optical_cd", "discogs_family", "Discogs label"), ("vinyl_12", "vinyl", '12" vinyl')]
    assert families == ["discogs_family", "optical", "vinyl"]


async def test_the_edges_roll_back_with_the_release_document(graph_connection: psycopg.AsyncConnection[Any]) -> None:
    # The rows are written on the message's own connection, so a failure anywhere in the
    # message takes them with it rather than leaving the graph describing media the catalog
    # never stored. `process_release` is driven inside its own nested transaction here, which
    # is the same shape the service's delivery handler wraps one message in.
    await _seed_discogs_rows(graph_connection)
    processor = _processor()

    with pytest.raises(RuntimeError):
        async with graph_connection.transaction():
            await processor.process_release(graph_connection, _release([{"format": "CD"}]))
            assert await _counts_by_source(graph_connection) == {"discogs": (2, 3), "musicbrainz": (1, 1)}
            raise RuntimeError("the message failed after the media write")

    assert await _counts_by_source(graph_connection) == {"discogs": (2, 3)}
