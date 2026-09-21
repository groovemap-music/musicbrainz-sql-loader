"""Real-PostgreSQL regressions proving the pinned schema revision applies cleanly.

The producer revision comes from ``groovemap-database-schema``, pinned as a dev
dependency on the same commit ``contracts/persistence/v1/source.json`` records
this repository as tested against
(``06629c6a7681127f74995bbae638ba048e2abd6d``). Applying the producer's own DDL,
rather than asserting against a hand-copied subset, is what keeps this test from
drifting behind the relations the loader writes: ``graph.issued_on``, the
``graph.medium`` and ``graph.media_family`` vertex tables it upserts into, the
``musicbrainz.relationships`` indexes the delete-reconciliation and parity work
read, and -- since the pin was promoted -- the ``updated_at`` column the purge
keys on.

The column assertion here is deliberately narrow: it states what the *schema*
declares. That the loader's startup probe then finds it and turns the purge on is
``tests/integration/test_delete_reconciliation.py``'s subject, because that is a
claim about the loader rather than about the DDL.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool
from groovemap_schema.postgres import create_postgres_schema
from psycopg.conninfo import conninfo_to_dict


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# The two endpoint-pair indexes plus the single-column relationship_type index
# musicbrainz.relationships carries for the delete-reconciliation and parity
# work this molecule adds next.
_RELATIONSHIP_INDEXES = (
    "idx_mb_rels_endpoint_source",
    "idx_mb_rels_endpoint_target",
    "idx_mb_rels_type",
)


# The delete-reconciliation column and its exact declared type, promoted with the
# pin. A narrower type would pass a name-only probe and then truncate the refresh,
# so the type is asserted here as well as probed by the loader.
_RECONCILIATION_COLUMNS = (
    ("relationships", "updated_at", "timestamp with time zone"),
    ("external_links", "updated_at", "timestamp with time zone"),
)


@pytest_asyncio.fixture
async def schema_pool() -> AsyncIterator[AsyncPostgreSQLPool]:
    """Apply the pinned groovemap-database-schema initializer to the integration container."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")

    connection_params: dict[str, Any] = conninfo_to_dict(database_url)
    pool = AsyncPostgreSQLPool(connection_params=connection_params, max_retries=1)
    await pool.initialize()
    failures = await create_postgres_schema(pool)
    assert failures == 0, f"{failures} schema statements failed against the integration container"
    try:
        yield pool
    finally:
        await pool.close()


async def _to_regclass(pool: AsyncPostgreSQLPool, qualified_name: str) -> bool:
    async with pool.connection() as conn:
        cursor = await conn.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified_name,))
        row = await cursor.fetchone()
        assert row is not None
        return bool(row[0])


async def test_graph_issued_on_table_exists(schema_pool: AsyncPostgreSQLPool) -> None:
    assert await _to_regclass(schema_pool, "graph.issued_on")


async def test_graph_medium_table_exists(schema_pool: AsyncPostgreSQLPool) -> None:
    assert await _to_regclass(schema_pool, "graph.medium")


async def test_graph_media_family_table_exists(schema_pool: AsyncPostgreSQLPool) -> None:
    assert await _to_regclass(schema_pool, "graph.media_family")


async def test_musicbrainz_relationships_carries_the_endpoint_and_type_indexes(
    schema_pool: AsyncPostgreSQLPool,
) -> None:
    async with schema_pool.connection() as conn:
        cursor = await conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'musicbrainz' AND tablename = 'relationships'",
        )
        rows = await cursor.fetchall()
    indexnames = {row[0] for row in rows}
    missing = set(_RELATIONSHIP_INDEXES) - indexnames
    assert not missing, f"musicbrainz.relationships is missing indexes: {sorted(missing)}"


async def test_graph_mb_relationship_type_function_exists(schema_pool: AsyncPostgreSQLPool) -> None:
    async with schema_pool.connection() as conn:
        cursor = await conn.execute("SELECT to_regprocedure('graph.mb_relationship_type(text)') IS NOT NULL")
        row = await cursor.fetchone()
    assert row is not None
    assert bool(row[0])


@pytest.mark.parametrize(("table", "column", "data_type"), _RECONCILIATION_COLUMNS)
async def test_the_promoted_schema_declares_the_delete_reconciliation_column(
    schema_pool: AsyncPostgreSQLPool,
    table: str,
    column: str,
    data_type: str,
) -> None:
    async with schema_pool.connection() as conn:
        cursor = await conn.execute(
            "SELECT data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'musicbrainz' AND table_name = %s AND column_name = %s",
            (table, column),
        )
        row = await cursor.fetchone()
    assert row is not None, f"musicbrainz.{table}.{column} is absent from the promoted schema"
    assert row[0] == data_type
    assert row[1] == "NO"
