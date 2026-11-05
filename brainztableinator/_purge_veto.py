"""The durable record of which extractions vetoed the delete-reconciliation.

The veto marks on ``StaleChildRowPurge`` are process memory, and dumps are a month apart, so they
cannot survive to be counted. This loader's rows in ``public.loader_extraction_latch`` can: one per
extraction, keyed by the signal's version, written whether the purge ran or not, so a clean
extraction ends a run of vetoes. ``purge_vetoed`` holds the entity types that vetoed it. The
relation belongs to ``groovemap-database-schema``, and this loader issues no DDL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


LOADER: Final = "musicbrainz"

_COLUMN_PRESENT = """
SELECT 1 FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'loader_extraction_latch' AND column_name = 'purge_vetoed'
"""

# A union, so a redelivered signal cannot shrink what an earlier delivery recorded.
_RECORD = """
INSERT INTO public.loader_extraction_latch AS latch (loader, version, purge_vetoed)
VALUES (%(loader)s, %(version)s, %(vetoed)s::text[])
ON CONFLICT (loader, version) DO UPDATE SET
    purge_vetoed = (SELECT coalesce(array_agg(DISTINCT vetoed ORDER BY vetoed), '{}') FROM unnest(latch.purge_vetoed || EXCLUDED.purge_vetoed) AS vetoed),
    updated_at = NOW()
"""

_READ = "SELECT purge_vetoed FROM public.loader_extraction_latch WHERE loader = %(loader)s AND version = %(version)s"

_HISTORY = "SELECT purge_vetoed FROM public.loader_extraction_latch WHERE loader = %(loader)s ORDER BY version DESC"


def extraction_key(data: Mapping[str, Any]) -> str | None:
    """Return the extraction a signal belongs to: its version, or its start if it has none."""
    version = str(data.get("version") or "").strip()
    return version or str(data.get("started_at") or "").strip() or None


async def purge_veto_column_present(connection_pool: Any) -> bool:
    """Report whether the schema declares the column this module writes."""
    async with connection_pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(_COLUMN_PRESENT)
        return await cursor.fetchone() is not None


async def recorded_vetoes(connection_pool: Any, version: str) -> list[str]:
    """Return the entity types an earlier delivery recorded for this extraction."""
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(_READ, {"loader": LOADER, "version": version})
            row = await cursor.fetchone()
    return list(row[0]) if row else []


async def record_extraction(connection_pool: Any, version: str, vetoed: Sequence[str]) -> None:
    """Record that this extraction reached its purge, with the types that vetoed it, if any."""
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(_RECORD, {"loader": LOADER, "version": version, "vetoed": list(vetoed)})


async def consecutive_vetoes(connection_pool: Any) -> int:
    """Return how many extractions in a row, newest first, vetoed the purge."""
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(_HISTORY, {"loader": LOADER})
            rows = await cursor.fetchall()
    consecutive = 0
    for (vetoed,) in rows:
        if not vetoed:
            break
        consecutive += 1
    return consecutive
