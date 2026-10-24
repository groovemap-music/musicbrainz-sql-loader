"""Real-PostgreSQL regressions for the link-change events the upserts record.

`public.identity_link_events` is the Identity Ledger's dated link history. A load that adds,
changes, or removes an existing row's Discogs id writes one `groovemap_load` event in the same
statement as the upsert. What has to hold against a real database, and cannot be shown with a
cursor double, is that the schema's own CHECKs and idempotence key accept what the statement
writes, that the event kind is read off the right pair of ids for each entity kind, and that the
event shares the upsert's fate.

Most tests run inside one transaction that is rolled back. The commit test is the exception: it
has to commit to prove visibility from another connection, so it removes its own rows.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
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
from brainztableinator._record_processing import UNVERSIONED_SOURCE_REF, MusicBrainzRecordProcessor


if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextlib import AbstractContextManager

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

MBID = "5b4c3d2e-1f0a-4b9c-8d7e-6f5a4b3c2d1e"
OTHER_MBID = "8a7b6c5d-4e3f-4a2b-9c1d-0e9f8a7b6c5d"
DUMP_VERSION = "20261021-001001"


@dataclass(frozen=True)
class Kind:
    """One of the four upserts, with the names it writes under."""

    entity_kind: str
    table: str
    id_field: str
    process: str
    upsert: str


# `entity_kind` is what `provider_aliases` holds for a MusicBrainz row, so a release group is
# a `master`; the other three are named as they are.
KINDS = (
    Kind("artist", "musicbrainz.artists", "discogs_artist_id", "process_artist", "upsert_artist"),
    Kind("label", "musicbrainz.labels", "discogs_label_id", "process_label", "upsert_label"),
    Kind("release", "musicbrainz.releases", "discogs_release_id", "process_release", "upsert_release"),
    Kind("master", "musicbrainz.release_groups", "discogs_master_id", "process_release_group", "upsert_release_group"),
)
EVERY_KIND = pytest.mark.parametrize("kind", KINDS, ids=[kind.entity_kind for kind in KINDS])


class _Observer:
    """The batch-telemetry seam, reduced to what the record processor calls on it."""

    def flush(self, _entity: str) -> AbstractContextManager[Any]:
        return MagicMock()

    def record(self, _entity: str, _size: int, _duration_s: float, _outcome: str) -> None:
        return None

    def set_outcome(self, _span: Any, _outcome: str) -> None:
        return None


def _processor() -> MusicBrainzRecordProcessor:
    return MusicBrainzRecordProcessor(PostgreSQLMusicBrainzWriter(), _Observer(), map_musicbrainz_release)


def _record(kind: Kind, discogs_id: int | None, *, mbid: str = MBID, dump_version: str | None = DUMP_VERSION) -> dict[str, Any]:
    """Return the event a producer would publish for this row naming this Discogs id."""
    record: dict[str, Any] = {"mbid": mbid, "name": "A Row", kind.id_field: discogs_id}
    if dump_version is not None:
        record["dump_version"] = dump_version
    return record


async def _load(conn: psycopg.AsyncConnection[Any], kind: Kind, discogs_id: int | None, **kwargs: Any) -> None:
    await getattr(_processor(), kind.process)(conn, _record(kind, discogs_id, **kwargs))


async def _events(conn: psycopg.AsyncConnection[Any], mbid: str = MBID) -> list[tuple[Any, ...]]:
    async with conn.cursor() as cursor:
        await cursor.execute(
            "SELECT entity_kind, event, discogs_id_before, discogs_id_after, dated_from, source_ref, provider, external_id "
            "FROM public.identity_link_events WHERE external_id = %s ORDER BY recorded_at, id",
            (mbid,),
        )
        return await cursor.fetchall()


async def _stored_id(conn: psycopg.AsyncConnection[Any], kind: Kind, mbid: str = MBID) -> int | None:
    async with conn.cursor() as cursor:
        await cursor.execute(f"SELECT {kind.id_field} FROM {kind.table} WHERE mbid = %s", (mbid,))  # noqa: S608 - fixed test constants
        row = await cursor.fetchone()
    assert row is not None
    return row[0]


@pytest_asyncio.fixture
async def database_url() -> AsyncIterator[str]:
    """Apply the pinned schema once, then hand out the integration database's URL."""
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")

    connection_params: dict[str, Any] = conninfo_to_dict(url)
    pool = AsyncPostgreSQLPool(connection_params=connection_params, max_retries=1)
    await pool.initialize()
    try:
        failures = await create_postgres_schema(pool)
        assert failures == 0, f"{failures} schema statements failed against the integration container"
    finally:
        await pool.close()
    yield url


@pytest_asyncio.fixture
async def conn(database_url: str) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
    """One connection whose work is always rolled back."""
    connection = await psycopg.AsyncConnection.connect(database_url)
    await connection.set_autocommit(False)
    try:
        yield connection
    finally:
        await connection.rollback()
        await connection.close()


@EVERY_KIND
async def test_a_brand_new_row_writes_no_event(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, 111)

    assert await _stored_id(conn, kind) == 111
    assert await _events(conn) == []


@EVERY_KIND
async def test_a_new_row_without_a_link_writes_no_event(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, None)

    assert await _stored_id(conn, kind) is None
    assert await _events(conn) == []


@EVERY_KIND
async def test_adding_a_link_writes_one_link_added_event(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, None)
    await _load(conn, kind, 222)

    assert await _events(conn) == [(kind.entity_kind, "link_added", None, 222, "groovemap_load", DUMP_VERSION, "musicbrainz", MBID)]


@EVERY_KIND
async def test_changing_a_link_writes_one_link_changed_event(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, 111)
    await _load(conn, kind, 222)

    assert await _events(conn) == [(kind.entity_kind, "link_changed", 111, 222, "groovemap_load", DUMP_VERSION, "musicbrainz", MBID)]
    assert await _stored_id(conn, kind) == 222


@EVERY_KIND
async def test_removing_a_link_writes_one_link_removed_event(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, 111)
    await _load(conn, kind, None)

    assert await _events(conn) == [(kind.entity_kind, "link_removed", 111, None, "groovemap_load", DUMP_VERSION, "musicbrainz", MBID)]
    assert await _stored_id(conn, kind) is None


@EVERY_KIND
async def test_an_unchanged_link_writes_no_event(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, 111)
    await _load(conn, kind, 111)

    assert await _events(conn) == []


@EVERY_KIND
async def test_reprocessing_a_message_writes_no_duplicate(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, 111)
    await _load(conn, kind, 222)
    # The broker redelivers the message whose transaction already committed. The row now holds
    # the id the message carries, so the second pass sees no change.
    await _load(conn, kind, 222)
    await _load(conn, kind, 222)

    assert len(await _events(conn)) == 1


@EVERY_KIND
async def test_events_are_scoped_to_their_own_row(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, 111)
    await _load(conn, kind, 111, mbid=OTHER_MBID)
    await _load(conn, kind, 222)

    assert [event[1] for event in await _events(conn)] == ["link_changed"]
    assert await _events(conn, OTHER_MBID) == []


@EVERY_KIND
async def test_the_event_is_dated_by_the_load_transaction(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, 111)
    await _load(conn, kind, 222)

    async with conn.cursor() as cursor:
        await cursor.execute(
            "SELECT event_at = transaction_timestamp(), decision_ref IS NULL FROM public.identity_link_events WHERE external_id = %s",
            (MBID,),
        )
        assert await cursor.fetchone() == (True, True)


@EVERY_KIND
async def test_a_message_without_a_dump_version_is_filed_under_the_unversioned_ref(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, 111, dump_version=None)
    await _load(conn, kind, 222, dump_version=None)

    assert [event[5] for event in await _events(conn)] == [UNVERSIONED_SOURCE_REF]


@EVERY_KIND
async def test_the_event_rolls_back_with_its_upsert(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, 111)

    class Abort(Exception):
        pass

    with pytest.raises(Abort):
        async with conn.transaction():
            await _load(conn, kind, 222)
            # Both writes are visible inside the transaction that made them.
            assert await _stored_id(conn, kind) == 222
            assert len(await _events(conn)) == 1
            raise Abort

    assert await _stored_id(conn, kind) == 111
    assert await _events(conn) == []


@EVERY_KIND
async def test_a_failing_event_rolls_back_the_upsert(conn: psycopg.AsyncConnection[Any], kind: Kind) -> None:
    await _load(conn, kind, 111)

    # A source_ref the NOT NULL column refuses makes the event insert fail. The upsert is the
    # same statement, so the row must not keep the id the failed statement tried to write.
    with pytest.raises(psycopg.errors.NotNullViolation):
        async with conn.transaction():
            await getattr(PostgreSQLMusicBrainzWriter(), kind.upsert)(conn, _writer_values(kind, 222), None)

    assert await _stored_id(conn, kind) == 111
    assert await _events(conn) == []


def _writer_values(kind: Kind, discogs_id: int) -> tuple[Any, ...]:
    """Return the values tuple the record processor hands the writer for this kind."""
    if kind.entity_kind == "artist":
        return (MBID, "n", "s", "t", "g", None, None, False, "a", "ba", "ea", "d", discogs_id, [], [], {}, None)
    if kind.entity_kind == "label":
        return (MBID, "n", "t", None, None, None, False, "a", "d", discogs_id, {}, None)
    if kind.entity_kind == "release":
        return (MBID, "n", None, "Official", None, discogs_id, {}, {}, None)
    return (MBID, "n", "t", [], None, "d", discogs_id, {}, None)


@EVERY_KIND
async def test_a_committed_upsert_leaves_its_event_visible_to_other_connections(database_url: str, kind: Kind) -> None:
    mbid = "0c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f"
    writer = await psycopg.AsyncConnection.connect(database_url)
    reader = await psycopg.AsyncConnection.connect(database_url)
    try:
        await writer.set_autocommit(False)
        async with writer.transaction():
            await _load(writer, kind, 111, mbid=mbid)
        async with writer.transaction():
            await _load(writer, kind, 222, mbid=mbid)

        assert await _events(reader, mbid) == [(kind.entity_kind, "link_changed", 111, 222, "groovemap_load", DUMP_VERSION, "musicbrainz", mbid)]
    finally:
        await writer.rollback()
        await writer.set_autocommit(True)
        await writer.execute("DELETE FROM public.identity_link_events WHERE external_id = %s", (mbid,))
        await writer.execute(f"DELETE FROM {kind.table} WHERE mbid = %s", (mbid,))  # noqa: S608 - fixed test constants
        await reader.close()
        await writer.close()
