"""Real-PostgreSQL regressions proving the pinned schema revision applies cleanly.

The producer revision comes from ``groovemap-database-schema``, pinned as a dev
dependency on the same commit ``contracts/persistence/v1/source.json`` records
this repository as tested against
(``ea36cfa66672cb1e3f565165fea56d01b9b19c95``). Applying the producer's own DDL,
rather than asserting against a hand-copied subset, is what keeps this test from
drifting behind the relations the loader will write once
gm-musicbrainz-sql-loader-0fc.3 lands: ``graph.issued_on``, the ``graph.medium``
and ``graph.media_family`` vertex tables it upserts into, and the
``musicbrainz.relationships`` indexes the delete-reconciliation and parity work
read.
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
