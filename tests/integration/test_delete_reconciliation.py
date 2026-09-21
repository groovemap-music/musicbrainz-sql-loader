"""Real-PostgreSQL regressions for the MusicBrainz child-row delete-reconciliation.

These run against the *promoted* producer schema, applied by
``groovemap-database-schema``'s own initializer at the revision
``contracts/persistence/v1/source.json`` records. That revision declares
``updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`` on both child-row tables, so no
test here issues the DDL the feature depends on: the column arrives with the pin,
the loader's startup probe finds it, and
``test_the_promoted_schema_enables_the_purge_and_a_removed_relationship_disappears``
is the end-to-end proof that a full import over the promoted schema removes the
relationship the second extraction stopped sending.

The degraded path is still covered, because it is still the guard. Two tests take
the promoted column away -- one dropping it, one narrowing it to ``DATE`` -- and
assert that the probe refuses to enable the purge rather than running one that
would delete live rows. Those are the only tests that issue DDL, and each restores
what it changed.

Everything the purge depends on is exercised end to end against a real server: that
an upsert of a still-present relationship refreshes ``updated_at`` even when it
changes nothing else, that the endpoint normalization means a relationship seen only
from its backward side still counts as refreshed, that a relationship removed
upstream is the one that disappears, and that a loader restart part-way through an
import does not delete what that import had already written.
"""

from __future__ import annotations

import os
import uuid
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
import structlog
from common import AsyncPostgreSQLPool
from common.media import map_musicbrainz_release
from groovemap_schema.postgres import create_postgres_schema
from psycopg.conninfo import conninfo_to_dict

from brainztableinator._persistence import PostgreSQLMusicBrainzWriter
from brainztableinator._reconciliation import StaleChildRowPurge, reconciliation_columns_present
from brainztableinator._record_processing import MusicBrainzRecordProcessor


if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

logger = structlog.get_logger(__name__)

ARTIST = str(uuid.UUID(int=0xA47157))
KEPT_TARGET = str(uuid.UUID(int=0x4EEB))
REMOVED_TARGET = str(uuid.UUID(int=0x4E0E4))
BACKWARD_TARGET = str(uuid.UUID(int=0xBAC4))

DATA_TYPES = ("artists", "labels", "release-groups", "releases")

RELATIONSHIPS = "musicbrainz.relationships"
EXTERNAL_LINKS = "musicbrainz.external_links"

# Restore-only DDL. The promoted schema declares this column; the two degraded-mode
# tests take it away to prove the probe refuses, and put it back exactly as the
# schema owner declares it so the next test sees the promoted shape again.
_ADD_UPDATED_AT = "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"


class _NullObserver:
    """The batch telemetry seam, reduced to nothing, so the writes stand alone."""

    def flush(self, entity: str) -> Any:
        """Open a span-shaped context that records nothing."""
        del entity
        return nullcontext(None)

    def record(self, entity: str, size: int, duration_s: float, outcome: str) -> None:
        """Discard the batch measurement."""
        del entity, size, duration_s, outcome

    def set_outcome(self, span: Any, outcome: str) -> None:
        """Discard the span outcome."""
        del span, outcome


def _relationship(target_mbid: str, relationship_type: str, direction: str | None = None) -> dict[str, Any]:
    relationship: dict[str, Any] = {
        "target_mbid": target_mbid,
        "target_type": "artist",
        "type": relationship_type,
        "attributes": [],
    }
    if direction is not None:
        relationship["direction"] = direction
    return relationship


def _signal(started_at: datetime) -> dict[str, Any]:
    """Build an ``extraction_complete`` body with every field the v1 schema requires."""
    return {
        "type": "extraction_complete",
        "version": "2026-09-18",
        "timestamp": started_at.isoformat(),
        "started_at": started_at.isoformat(),
        "record_counts": dict.fromkeys(DATA_TYPES, 1),
    }


def _latched(pool: AsyncPostgreSQLPool, started_at: datetime) -> StaleChildRowPurge:
    """Return a purge with all four extraction_complete signals collected."""
    purge = StaleChildRowPurge(pool, logger)
    for data_type in DATA_TYPES:
        purge.record_completion(data_type, _signal(started_at))
    assert purge.is_latched()
    return purge


@pytest_asyncio.fixture
async def promoted_schema_pool() -> AsyncIterator[AsyncPostgreSQLPool]:
    """The promoted producer schema, emptied of the relations these tests write."""
    pool = await _applied_schema_pool()
    try:
        await _truncate(pool)
        yield pool
    finally:
        await _truncate(pool)
        await pool.close()


async def _applied_schema_pool() -> AsyncPostgreSQLPool:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")

    connection_params: dict[str, Any] = conninfo_to_dict(database_url)
    pool = AsyncPostgreSQLPool(connection_params=connection_params, max_retries=1)
    await pool.initialize()
    failures = await create_postgres_schema(pool)
    assert failures == 0, f"{failures} schema statements failed against the integration container"
    return pool


async def _truncate(pool: AsyncPostgreSQLPool) -> None:
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        await conn.execute("TRUNCATE musicbrainz.relationships, musicbrainz.external_links")
        await conn.execute("TRUNCATE musicbrainz.artists, musicbrainz.labels, musicbrainz.releases, musicbrainz.release_groups")
        await conn.execute("TRUNCATE graph.issued_on, graph.medium, graph.media_family")


def _processor() -> MusicBrainzRecordProcessor:
    """A processor whose writer refreshes updated_at, as the startup probe would enable."""
    writer = PostgreSQLMusicBrainzWriter()
    writer.set_refresh_updated_at(True)
    return MusicBrainzRecordProcessor(writer, _NullObserver(), lambda record: record)


async def _relationship_targets(pool: AsyncPostgreSQLPool) -> set[str]:
    async with pool.connection() as conn:
        cursor = await conn.execute("SELECT target_mbid::text FROM musicbrainz.relationships")
        rows = await cursor.fetchall()
    return {row[0] for row in rows}


async def _now(pool: AsyncPostgreSQLPool) -> datetime:
    async with pool.connection() as conn:
        cursor = await conn.execute("SELECT NOW()")
        row = await cursor.fetchone()
    assert row is not None
    started_at: datetime = row[0]
    return started_at


async def _age_every_row(pool: AsyncPostgreSQLPool) -> None:
    """Backdate both tables, standing in for the rows a previous extraction left behind."""
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        await conn.execute("UPDATE musicbrainz.relationships SET updated_at = NOW() - INTERVAL '1 day'")
        await conn.execute("UPDATE musicbrainz.external_links SET updated_at = NOW() - INTERVAL '1 day'")


async def test_the_probe_reports_the_column_absent_against_an_unpromoted_schema(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """A deployment still on a pre-promotion schema keeps loading with the purge off.

    The column is dropped and put back here rather than the pin being un-promoted, so
    this states what the loader does against a schema that lacks it. The reverse case
    is every other test in this module: they run on the promoted schema untouched.
    """
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        for table in (RELATIONSHIPS, EXTERNAL_LINKS):
            await conn.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS updated_at")

    try:
        assert await reconciliation_columns_present(promoted_schema_pool, logger) is False
    finally:
        async with promoted_schema_pool.connection() as conn:
            await conn.set_autocommit(True)
            for table in (RELATIONSHIPS, EXTERNAL_LINKS):
                await conn.execute(_ADD_UPDATED_AT.format(table=table))


async def test_the_probe_reports_the_column_present_on_the_promoted_schema(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """No DDL is issued here at all: the promoted pin is what the probe finds."""
    assert await reconciliation_columns_present(promoted_schema_pool, logger) is True


async def test_a_relationship_removed_upstream_disappears(promoted_schema_pool: AsyncPostgreSQLPool) -> None:
    """The acceptance case: the run re-sends one relationship and the other is deleted."""
    processor = _processor()
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(REMOVED_TARGET, "collaboration")],
        )
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET, REMOVED_TARGET}

    # Everything in the table predates the extraction that is about to start.
    await _age_every_row(promoted_schema_pool)
    started_at = await _now(promoted_schema_pool)

    # This extraction's dump no longer carries the collaboration, only the membership.
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])

    deleted = await _latched(promoted_schema_pool, started_at).purge()

    assert deleted[RELATIONSHIPS] == 1
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET}


async def test_a_restart_mid_import_does_not_delete_what_that_import_wrote(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """The regression a process-start boundary caused.

    The extraction starts, the loader writes part of it, the loader restarts, and the
    rest arrives. Because the boundary is the extraction's ``started_at`` and not
    anything this process measured, the restart moves nothing and the rows written
    before it survive. A ``SELECT NOW()`` boundary taken at the restart would have sat
    after them and deleted every one.
    """
    processor = _processor()
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(REMOVED_TARGET, "collaboration")])

    await _age_every_row(promoted_schema_pool)
    started_at = await _now(promoted_schema_pool)

    # First half of the import, written by the process that is about to die.
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])

    # The restart: a brand-new purge, with no memory of anything before it.
    restarted = StaleChildRowPurge(promoted_schema_pool, logger)

    # Second half of the import, written after the restart.
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(BACKWARD_TARGET, "supporting musician")])

    for data_type in DATA_TYPES:
        restarted.record_completion(data_type, _signal(started_at))
    deleted = await restarted.purge()

    assert restarted.boundary == started_at
    assert deleted[RELATIONSHIPS] == 1
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET, BACKWARD_TARGET}


async def test_a_second_extraction_re_latches_on_its_own_boundary(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """A long-lived loader reconciles each extraction against that extraction's start."""
    processor = _processor()
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(REMOVED_TARGET, "collaboration")],
        )

    await _age_every_row(promoted_schema_pool)
    first_started_at = await _now(promoted_schema_pool)

    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(REMOVED_TARGET, "collaboration")],
        )

    purge = _latched(promoted_schema_pool, first_started_at)
    assert await purge.purge() == {RELATIONSHIPS: 0, EXTERNAL_LINKS: 0}

    # A second extraction, later, in the same long-lived process. Its first signal must
    # re-latch rather than re-fire the first extraction's purge.
    second_started_at = await _now(promoted_schema_pool)
    purge.record_completion("artists", _signal(second_started_at))
    assert purge.boundary == second_started_at
    assert await purge.purge() == {}
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET, REMOVED_TARGET}

    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])

    for data_type in DATA_TYPES[1:]:
        purge.record_completion(data_type, _signal(second_started_at))

    assert (await purge.purge())[RELATIONSHIPS] == 1
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET}


async def test_an_unchanged_upsert_refreshes_updated_at(promoted_schema_pool: AsyncPostgreSQLPool) -> None:
    """Re-sending an identical relationship must mark it as still present.

    This is the regression the purge would otherwise turn destructive: the conflict
    clause changes no other column, so if it did not touch ``updated_at`` every live
    relationship would read as stale.
    """
    processor = _processor()
    links = [{"url": "https://example.invalid/artist", "service": "official homepage"}]
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])
        await processor.insert_external_links(conn, ARTIST, "artist", links)

    await _age_every_row(promoted_schema_pool)
    started_at = await _now(promoted_schema_pool)

    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])
        await processor.insert_external_links(conn, ARTIST, "artist", links)

    deleted = await _latched(promoted_schema_pool, started_at).purge()

    assert deleted == {RELATIONSHIPS: 0, EXTERNAL_LINKS: 0}
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET}


async def test_a_backward_relationship_refreshes_the_canonical_row(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """Seeing a relationship only from its backward side still saves it from the purge.

    ``insert_relationships`` swaps the endpoints for ``direction: backward``, so the
    row written from either side is the one the relationships_natural_key contract
    names. If the swap or the key drifted, the second pass would insert a second row
    and the purge would delete the first as stale.
    """
    processor = _processor()
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        # First seen from BACKWARD_TARGET's own message, in the forward direction.
        await processor.insert_relationships(conn, BACKWARD_TARGET, "artist", [_relationship(ARTIST, "member of band")])

    await _age_every_row(promoted_schema_pool)
    started_at = await _now(promoted_schema_pool)

    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        # This extraction only sees it from ARTIST's message, reported backward.
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(BACKWARD_TARGET, "member of band", direction="backward")],
        )

    async with promoted_schema_pool.connection() as conn:
        cursor = await conn.execute("SELECT count(*) FROM musicbrainz.relationships")
        row = await cursor.fetchone()
    assert row is not None
    assert row[0] == 1, "the backward report must refresh the canonical row, not insert a second one"

    deleted = await _latched(promoted_schema_pool, started_at).purge()

    assert deleted[RELATIONSHIPS] == 0
    assert await _relationship_targets(promoted_schema_pool) == {ARTIST}


async def test_the_delete_fraction_cap_refuses_a_whole_table_shrink(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """Nothing is refreshed this extraction, so the cap must stop the tables being emptied."""
    processor = _processor()
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(REMOVED_TARGET, "collaboration")],
        )

    await _age_every_row(promoted_schema_pool)
    started_at = await _now(promoted_schema_pool)

    deleted = await _latched(promoted_schema_pool, started_at).purge()

    assert deleted[RELATIONSHIPS] == 0
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET, REMOVED_TARGET}


async def test_the_purge_is_idempotent(promoted_schema_pool: AsyncPostgreSQLPool) -> None:
    """A second pass over the reconciled tables deletes nothing more."""
    processor = _processor()
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [
                _relationship(KEPT_TARGET, "member of band"),
                _relationship(REMOVED_TARGET, "collaboration"),
                _relationship(BACKWARD_TARGET, "supporting musician"),
            ],
        )

    await _age_every_row(promoted_schema_pool)
    started_at = await _now(promoted_schema_pool)

    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(BACKWARD_TARGET, "supporting musician")],
        )

    purge = _latched(promoted_schema_pool, started_at)
    assert (await purge.purge())[RELATIONSHIPS] == 1

    # Re-collecting the same extraction's signals must not delete anything further.
    replayed = _latched(promoted_schema_pool, started_at)
    assert (await replayed.purge())[RELATIONSHIPS] == 0
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET, BACKWARD_TARGET}


async def test_a_dead_letter_vetoes_the_purge_against_a_real_table(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """A vetoed extraction must leave every row in place, stale or not."""
    processor = _processor()
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(REMOVED_TARGET, "collaboration")],
        )

    await _age_every_row(promoted_schema_pool)
    started_at = await _now(promoted_schema_pool)

    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])

    purge = _latched(promoted_schema_pool, started_at)
    purge.record_dead_letter("artists")

    assert await purge.purge() == {}
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET, REMOVED_TARGET}


async def test_a_writer_without_the_refresh_leaves_rows_looking_stale(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """Proves the conditional clause is what saves live rows, not the purge's own logic."""
    unrefreshing = MusicBrainzRecordProcessor(PostgreSQLMusicBrainzWriter(), _NullObserver(), lambda record: record)
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await unrefreshing.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(REMOVED_TARGET, "collaboration")],
        )

    await _age_every_row(promoted_schema_pool)
    started_at = await _now(promoted_schema_pool)

    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await unrefreshing.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])

    async with promoted_schema_pool.connection() as conn:
        cursor = await conn.execute("SELECT count(*) FROM musicbrainz.relationships WHERE updated_at < %s", (started_at,))
        row = await cursor.fetchone()
    assert row is not None
    assert row[0] == 2, "without the refresh clause the re-sent relationship still reads as stale"


async def test_the_probe_rejects_a_column_declared_with_a_narrower_type(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """A `date` column passes a name-only probe and then deletes live rows.

    `updated_at = NOW()` into a `date` truncates to day granularity through the
    assignment cast, so a row refreshed hours ago compares as older than a boundary
    taken earlier the same day. This asserts against a real server that the probe
    refuses the column rather than enabling a purge that would silently delete it.
    """
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        for table in (RELATIONSHIPS, EXTERNAL_LINKS):
            await conn.execute(f"ALTER TABLE {table} ALTER COLUMN updated_at TYPE DATE")

    try:
        assert await reconciliation_columns_present(promoted_schema_pool, logger) is False
    finally:
        async with promoted_schema_pool.connection() as conn:
            await conn.set_autocommit(True)
            for table in (RELATIONSHIPS, EXTERNAL_LINKS):
                await conn.execute(f"ALTER TABLE {table} ALTER COLUMN updated_at TYPE TIMESTAMPTZ")


async def test_a_truncating_column_would_have_deleted_a_live_row(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """Shows the loss the probe prevents, so the type check is not merely cosmetic."""
    processor = _processor()
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        for table in (RELATIONSHIPS, EXTERNAL_LINKS):
            await conn.execute(f"ALTER TABLE {table} ALTER COLUMN updated_at TYPE DATE")

    try:
        async with promoted_schema_pool.connection() as conn:
            await conn.set_autocommit(True)
            await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])
            # A boundary later in the same day than the truncated refresh.
            cursor = await conn.execute("SELECT date_trunc('day', NOW()) + INTERVAL '1 hour'")
            row = await cursor.fetchone()
            assert row is not None
            boundary = row[0]

            await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])
            cursor = await conn.execute("SELECT count(*) FROM musicbrainz.relationships WHERE updated_at < %s", (boundary,))
            stale = await cursor.fetchone()

        assert stale is not None
        assert stale[0] == 1, "a truncated updated_at reads as stale, which is what the probe refuses to enable"
    finally:
        async with promoted_schema_pool.connection() as conn:
            await conn.set_autocommit(True)
            for table in (RELATIONSHIPS, EXTERNAL_LINKS):
                await conn.execute(f"ALTER TABLE {table} ALTER COLUMN updated_at TYPE TIMESTAMPTZ")


async def test_a_repeated_signal_at_a_vetoed_boundary_leaves_every_row_in_place(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """At-least-once delivery must not defeat the dead-letter veto on a real table."""
    processor = _processor()
    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(REMOVED_TARGET, "collaboration")],
        )

    await _age_every_row(promoted_schema_pool)
    started_at = await _now(promoted_schema_pool)

    async with promoted_schema_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])

    purge = _latched(promoted_schema_pool, started_at)
    purge.record_dead_letter("artists")
    assert await purge.purge() == {}

    # The broker redelivers every signal at the same boundary.
    for data_type in DATA_TYPES:
        purge.record_completion(data_type, _signal(started_at))

    assert await purge.purge() == {}
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET, REMOVED_TARGET}


# One record per MusicBrainz entity kind, so the import below is a *full* one: all
# four kinds write into the same two child tables, which is the whole reason the
# purge waits for four `extraction_complete` signals before it fires.
LABEL = str(uuid.UUID(int=0x1ABE1))
RELEASE_GROUP = str(uuid.UUID(int=0x69))
RELEASE = str(uuid.UUID(int=0x4E1EA5E))


def _full_import(*, carrying_the_removed_relationship: bool) -> list[tuple[str, dict[str, Any]]]:
    """One record per entity kind, optionally still naming the relationship upstream drops."""
    artist_relations = [_relationship(KEPT_TARGET, "member of band")]
    if carrying_the_removed_relationship:
        artist_relations.append(_relationship(REMOVED_TARGET, "collaboration"))

    return [
        ("artists", {"id": ARTIST, "mbid": ARTIST, "name": "Reconciled Artist", "relations": artist_relations}),
        ("labels", {"id": LABEL, "mbid": LABEL, "name": "Reconciled Label", "relations": [_relationship(KEPT_TARGET, "label founder")]}),
        (
            "release-groups",
            {"id": RELEASE_GROUP, "mbid": RELEASE_GROUP, "name": "Reconciled Group", "relations": [_relationship(ARTIST, "tribute")]},
        ),
        (
            "releases",
            {
                "id": RELEASE,
                "mbid": RELEASE,
                "name": "Reconciled Release",
                "media_raw": [{"format": '12" Vinyl', "position": 1, "track_count": 9}],
                "relations": [_relationship(ARTIST, "supporting musician")],
            },
        ),
    ]


async def _run_import(pool: AsyncPostgreSQLPool, processor: MusicBrainzRecordProcessor, records: list[tuple[str, dict[str, Any]]]) -> None:
    """Write every record through the same per-message transaction the service uses."""
    handlers = {
        "artists": processor.process_artist,
        "labels": processor.process_label,
        "release-groups": processor.process_release_group,
        "releases": processor.process_release,
    }
    for data_type, record in records:
        async with pool.connection() as conn:
            await conn.set_autocommit(False)
            async with conn.transaction():
                await handlers[data_type](conn, record)


async def test_the_promoted_schema_enables_the_purge_and_a_removed_relationship_disappears(
    promoted_schema_pool: AsyncPostgreSQLPool,
) -> None:
    """The acceptance case for the promoted pin, with nothing about it stubbed.

    No DDL is issued: the column comes from the schema the pin now names. The probe
    answers against that schema, and its answer -- not a hardcoded ``True`` -- is what
    arms the writer, exactly as ``main`` does at startup. Then a full import of all
    four entity kinds runs twice, the second pass no longer naming one of the artist's
    relationships, and the purge deletes that row and only that row.
    """
    enabled = await reconciliation_columns_present(promoted_schema_pool, logger)
    assert enabled is True, "the promoted schema must satisfy the startup probe"

    writer = PostgreSQLMusicBrainzWriter()
    writer.set_refresh_updated_at(enabled)
    assert writer.refresh_updated_at is True
    processor = MusicBrainzRecordProcessor(writer, _NullObserver(), map_musicbrainz_release)

    await _run_import(promoted_schema_pool, processor, _full_import(carrying_the_removed_relationship=True))
    assert await _relationship_targets(promoted_schema_pool) >= {KEPT_TARGET, REMOVED_TARGET}

    # Everything written so far belongs to the extraction that is now finished with.
    await _age_every_row(promoted_schema_pool)
    started_at = await _now(promoted_schema_pool)

    # The next extraction's dump no longer carries the collaboration.
    await _run_import(promoted_schema_pool, processor, _full_import(carrying_the_removed_relationship=False))

    purge = StaleChildRowPurge(promoted_schema_pool, logger)
    for data_type in DATA_TYPES[:-1]:
        purge.record_completion(data_type, _signal(started_at))
        assert not purge.is_latched(), "the purge must wait for every entity kind"
        assert await purge.purge() == {}, "an unlatched purge must delete nothing"

    purge.record_completion(DATA_TYPES[-1], _signal(started_at))
    deleted = await purge.purge()

    assert deleted[RELATIONSHIPS] == 1
    assert await _relationship_targets(promoted_schema_pool) == {KEPT_TARGET, ARTIST}
