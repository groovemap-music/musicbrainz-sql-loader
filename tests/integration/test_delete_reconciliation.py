"""Real-PostgreSQL regressions for the MusicBrainz child-row delete-reconciliation.

These run against the pinned producer schema, applied by
``groovemap-database-schema``'s own initializer, plus the additive ``updated_at``
column ``_reconciliation.ensure_reconciliation_columns`` carries. Everything the
purge depends on is exercised end to end against a real server: that an upsert of
a still-present relationship refreshes ``updated_at`` even when it changes nothing
else, that the endpoint normalization means a relationship seen only from its
backward side still counts as refreshed, and that a relationship removed upstream
is the one that disappears.
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
from groovemap_schema.postgres import create_postgres_schema
from psycopg.conninfo import conninfo_to_dict

from brainztableinator._persistence import PostgreSQLMusicBrainzWriter
from brainztableinator._reconciliation import StaleChildRowPurge, ensure_reconciliation_columns
from brainztableinator._record_processing import MusicBrainzRecordProcessor


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

logger = structlog.get_logger(__name__)

ARTIST = str(uuid.UUID(int=0xA47157))
KEPT_TARGET = str(uuid.UUID(int=0x4EEB))
REMOVED_TARGET = str(uuid.UUID(int=0x4E0E4))
BACKWARD_TARGET = str(uuid.UUID(int=0xBAC4))


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


@pytest_asyncio.fixture
async def reconciliation_pool() -> AsyncIterator[AsyncPostgreSQLPool]:
    """Apply the pinned schema plus the reconciliation column, on a clean pair of tables."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")

    connection_params: dict[str, Any] = conninfo_to_dict(database_url)
    pool = AsyncPostgreSQLPool(connection_params=connection_params, max_retries=1)
    await pool.initialize()
    failures = await create_postgres_schema(pool)
    assert failures == 0, f"{failures} schema statements failed against the integration container"
    await ensure_reconciliation_columns(pool, logger)
    try:
        await _truncate(pool)
        yield pool
    finally:
        await _truncate(pool)
        await pool.close()


async def _truncate(pool: AsyncPostgreSQLPool) -> None:
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        await conn.execute("TRUNCATE musicbrainz.relationships, musicbrainz.external_links")


def _processor() -> MusicBrainzRecordProcessor:
    return MusicBrainzRecordProcessor(PostgreSQLMusicBrainzWriter(), _NullObserver(), lambda record: record)


async def _relationship_targets(pool: AsyncPostgreSQLPool) -> set[str]:
    async with pool.connection() as conn:
        cursor = await conn.execute("SELECT target_mbid::text FROM musicbrainz.relationships")
        rows = await cursor.fetchall()
    return {row[0] for row in rows}


async def _age_every_row(pool: AsyncPostgreSQLPool) -> None:
    """Backdate both tables, standing in for the rows a previous run left behind."""
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        await conn.execute("UPDATE musicbrainz.relationships SET updated_at = NOW() - INTERVAL '1 day'")
        await conn.execute("UPDATE musicbrainz.external_links SET updated_at = NOW() - INTERVAL '1 day'")


async def test_a_relationship_removed_upstream_disappears(reconciliation_pool: AsyncPostgreSQLPool) -> None:
    """The acceptance case: the run re-sends one relationship and the other is deleted."""
    processor = _processor()
    async with reconciliation_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(REMOVED_TARGET, "collaboration")],
        )
    assert await _relationship_targets(reconciliation_pool) == {KEPT_TARGET, REMOVED_TARGET}

    # Everything in the table predates the run that is about to start.
    await _age_every_row(reconciliation_pool)

    purge = StaleChildRowPurge(reconciliation_pool, logger)
    await purge.latch_run_start()

    # This run's dump no longer carries the collaboration, only the membership.
    async with reconciliation_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])

    deleted = await purge.purge(processed_records=1)

    assert deleted["musicbrainz.relationships"] == 1
    assert await _relationship_targets(reconciliation_pool) == {KEPT_TARGET}


async def test_an_unchanged_upsert_refreshes_updated_at(reconciliation_pool: AsyncPostgreSQLPool) -> None:
    """Re-sending an identical relationship must mark it as still present.

    This is the regression the purge would otherwise turn destructive: the conflict
    clause changes no other column, so if it did not touch ``updated_at`` every live
    relationship would read as stale.
    """
    processor = _processor()
    async with reconciliation_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])
        await processor.insert_external_links(
            conn,
            ARTIST,
            "artist",
            [{"url": "https://example.invalid/artist", "service": "official homepage"}],
        )

    await _age_every_row(reconciliation_pool)

    purge = StaleChildRowPurge(reconciliation_pool, logger)
    await purge.latch_run_start()

    async with reconciliation_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])
        await processor.insert_external_links(
            conn,
            ARTIST,
            "artist",
            [{"url": "https://example.invalid/artist", "service": "official homepage"}],
        )

    deleted = await purge.purge(processed_records=1)

    assert deleted == {"musicbrainz.relationships": 0, "musicbrainz.external_links": 0}
    assert await _relationship_targets(reconciliation_pool) == {KEPT_TARGET}


async def test_a_backward_relationship_refreshes_the_canonical_row(
    reconciliation_pool: AsyncPostgreSQLPool,
) -> None:
    """Seeing a relationship only from its backward side still saves it from the purge.

    ``insert_relationships`` swaps the endpoints for ``direction: backward``, so the
    row written from either side is the one the relationships_natural_key contract
    names. If the swap or the key drifted, the second pass would insert a second row
    and the purge would delete the first as stale.
    """
    processor = _processor()
    async with reconciliation_pool.connection() as conn:
        await conn.set_autocommit(True)
        # First seen from BACKWARD_TARGET's own message, in the forward direction.
        await processor.insert_relationships(conn, BACKWARD_TARGET, "artist", [_relationship(ARTIST, "member of band")])

    await _age_every_row(reconciliation_pool)

    purge = StaleChildRowPurge(reconciliation_pool, logger)
    await purge.latch_run_start()

    async with reconciliation_pool.connection() as conn:
        await conn.set_autocommit(True)
        # This run only sees it from ARTIST's message, reported backward.
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(BACKWARD_TARGET, "member of band", direction="backward")],
        )

    async with reconciliation_pool.connection() as conn:
        cursor = await conn.execute("SELECT count(*) FROM musicbrainz.relationships")
        row = await cursor.fetchone()
    assert row is not None
    assert row[0] == 1, "the backward report must refresh the canonical row, not insert a second one"

    deleted = await purge.purge(processed_records=1)

    assert deleted["musicbrainz.relationships"] == 0
    assert await _relationship_targets(reconciliation_pool) == {ARTIST}


async def test_the_delete_fraction_cap_refuses_a_whole_table_shrink(
    reconciliation_pool: AsyncPostgreSQLPool,
) -> None:
    """Nothing is refreshed this run, so the cap must stop the tables being emptied."""
    processor = _processor()
    async with reconciliation_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(REMOVED_TARGET, "collaboration")],
        )

    await _age_every_row(reconciliation_pool)

    purge = StaleChildRowPurge(reconciliation_pool, logger)
    await purge.latch_run_start()

    deleted = await purge.purge(processed_records=1)

    assert deleted["musicbrainz.relationships"] == 0
    assert await _relationship_targets(reconciliation_pool) == {KEPT_TARGET, REMOVED_TARGET}


async def test_the_purge_is_idempotent(reconciliation_pool: AsyncPostgreSQLPool) -> None:
    """A second pass over the reconciled tables deletes nothing more."""
    processor = _processor()
    async with reconciliation_pool.connection() as conn:
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

    await _age_every_row(reconciliation_pool)

    purge = StaleChildRowPurge(reconciliation_pool, logger)
    await purge.latch_run_start()

    async with reconciliation_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(BACKWARD_TARGET, "supporting musician")],
        )

    assert (await purge.purge(processed_records=1))["musicbrainz.relationships"] == 1
    assert (await purge.purge(processed_records=1))["musicbrainz.relationships"] == 0
    assert await _relationship_targets(reconciliation_pool) == {KEPT_TARGET, BACKWARD_TARGET}


async def test_a_dead_letter_vetoes_the_purge_against_a_real_table(
    reconciliation_pool: AsyncPostgreSQLPool,
) -> None:
    """A vetoed run must leave every row in place, stale or not."""
    processor = _processor()
    async with reconciliation_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(
            conn,
            ARTIST,
            "artist",
            [_relationship(KEPT_TARGET, "member of band"), _relationship(REMOVED_TARGET, "collaboration")],
        )

    await _age_every_row(reconciliation_pool)

    purge = StaleChildRowPurge(reconciliation_pool, logger)
    await purge.latch_run_start()

    async with reconciliation_pool.connection() as conn:
        await conn.set_autocommit(True)
        await processor.insert_relationships(conn, ARTIST, "artist", [_relationship(KEPT_TARGET, "member of band")])

    purge.record_dead_letter("artists")

    assert await purge.purge(processed_records=1) == {}
    assert await _relationship_targets(reconciliation_pool) == {KEPT_TARGET, REMOVED_TARGET}
