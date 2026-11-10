"""Rebuild the cross-provenance graph relations once a MusicBrainz extraction has completed.

``graph.artist_member_of`` holds a MusicBrainz half and ``graph.vertex_degree`` sums it, so a load
that adds or removes a member-of relationship leaves both stale until something rebuilds them. The
persistence contract names this loader, beside ``discogs-sql-loader``, a refresh owner of
``graph.refresh_artist_member_of()`` and then ``graph.refresh_vertex_degree()`` (``refresh_order``),
each on its own ``extraction_complete`` latch. Both functions take a transaction-scoped advisory
lock (``refresh_lock``), and the pair runs in one transaction here, so a Discogs refresh queues
behind it instead of interleaving with it.

The job is recorded in ``public.loader_derived_refresh_job`` under ``loader = 'musicbrainz'``, the
shape ``discogs-sql-loader`` uses, in three transactions: claim (a cursor generation, the latch
generation, a leased job), refresh (both functions, then the job completed and the latch stamped
``refreshed_at`` together), and, only on failure, a retry mark. A failure therefore leaves every
relation as it was and the job in ``retry`` with its error, and the caller requeues the
``extraction_complete`` so the same pass runs again. A stamped extraction is never refreshed twice.

The relations belong to ``groovemap-database-schema``; this module issues no DDL.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Final
from uuid import UUID, uuid4

from brainztableinator._purge_veto import LOADER


if TYPE_CHECKING:
    from collections.abc import Mapping

CURSOR: Final = "public.loader_derived_refresh_cursor"
JOB: Final = "public.loader_derived_refresh_job"
LATCH: Final = "public.loader_extraction_latch"

# The schema's refresh functions, in the contract's `refresh_order`: the degree sums the union.
REFRESH_FUNCTIONS: Final = ("refresh_artist_member_of", "refresh_vertex_degree")
_REFRESH_SQL: Final = {
    "refresh_artist_member_of": "SELECT * FROM graph.refresh_artist_member_of()",
    "refresh_vertex_degree": "SELECT * FROM graph.refresh_vertex_degree()",
}

# A pass is one TRUNCATE-and-INSERT pair and may queue behind a Discogs refresh on the advisory
# lock, so the lease is generous; an expired lease is what lets a redelivery take a crashed pass over.
LEASE_SECONDS: Final = 1800
_MAX_BACKOFF_SECONDS: Final = 300
_OWNER: Final = "musicbrainz-sql-loader"

_REQUIRED_TABLES: Final = ("loader_extraction_latch", "loader_derived_refresh_cursor", "loader_derived_refresh_job")
_REQUIRED_FUNCTIONS: Final = REFRESH_FUNCTIONS


class RefreshInProgress(RuntimeError):
    """Another delivery holds an unexpired lease on this extraction's refresh."""


class SupersededRefresh(RuntimeError):
    """A newer extraction or another claimant fenced this pass; its result must not be stamped."""


async def derived_refresh_available(connection_pool: Any) -> bool:
    """Report whether the schema declares the relations and functions this module uses."""
    async with connection_pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ANY(%s)",
            (list(_REQUIRED_TABLES),),
        )
        tables = (await cursor.fetchone())[0]
        await cursor.execute(
            "SELECT count(DISTINCT proname) FROM pg_proc JOIN pg_namespace ns ON ns.oid = pronamespace WHERE ns.nspname = 'graph' AND proname = ANY(%s)",
            (list(_REQUIRED_FUNCTIONS),),
        )
        functions = (await cursor.fetchone())[0]
    return bool(tables == len(_REQUIRED_TABLES) and functions == len(_REQUIRED_FUNCTIONS))


async def _claim(connection_pool: Any, version: str) -> tuple[int, UUID, int] | None:
    """Lock the cursor and latch, and lease the job; ``None`` when the extraction is already refreshed."""
    token = uuid4()
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute("INSERT INTO public.loader_derived_refresh_cursor (loader) VALUES (%s) ON CONFLICT (loader) DO NOTHING", (LOADER,))
            await cursor.execute("SELECT generation, version FROM public.loader_derived_refresh_cursor WHERE loader = %s FOR UPDATE", (LOADER,))
            cursor_generation, cursor_version = await cursor.fetchone()
            await cursor.execute(
                "INSERT INTO public.loader_extraction_latch (loader, version) VALUES (%s, %s) ON CONFLICT (loader, version) DO NOTHING",
                (LOADER, version),
            )
            await cursor.execute(
                "SELECT generation, refreshed_at FROM public.loader_extraction_latch WHERE loader = %s AND version = %s FOR UPDATE", (LOADER, version)
            )
            generation, refreshed_at = await cursor.fetchone()
            if refreshed_at is not None:
                return None
            if generation is None:
                if cursor_version == version:
                    generation = cursor_generation
                else:
                    generation = cursor_generation + 1
                    await cursor.execute(
                        "UPDATE public.loader_derived_refresh_cursor SET generation = %s, version = %s, updated_at = NOW() WHERE loader = %s",
                        (generation, version, LOADER),
                    )
                    await cursor.execute(
                        "UPDATE public.loader_derived_refresh_job SET state = 'superseded', next_attempt_at = NULL, lease_owner = NULL, lease_token = NULL, "
                        "lease_expires_at = NULL, superseded_at = NOW(), updated_at = NOW() "
                        "WHERE loader = %s AND generation < %s AND state IN ('pending', 'retry', 'leased')",
                        (LOADER, generation),
                    )
                await cursor.execute(
                    "UPDATE public.loader_extraction_latch SET generation = %s, updated_at = NOW() WHERE loader = %s AND version = %s",
                    (generation, LOADER, version),
                )
            await cursor.execute(
                "INSERT INTO public.loader_derived_refresh_job (loader, version, generation) VALUES (%s, %s, %s) ON CONFLICT (loader, version) DO NOTHING",
                (LOADER, version, generation),
            )
            await cursor.execute(
                "UPDATE public.loader_derived_refresh_job SET state = 'leased', attempt_count = attempt_count + 1, lease_epoch = lease_epoch + 1, "
                "lease_owner = %s, lease_token = %s, lease_expires_at = NOW() + (%s * INTERVAL '1 second'), "
                "next_attempt_at = NULL, started_at = NOW(), updated_at = NOW() "
                "WHERE loader = %s AND version = %s AND (state IN ('pending', 'retry') "
                "OR (state = 'leased' AND lease_expires_at < NOW())) RETURNING lease_epoch, attempt_count",
                (_OWNER, token, LEASE_SECONDS, LOADER, version),
            )
            row = await cursor.fetchone()
            if row is None:
                await cursor.execute("SELECT state FROM public.loader_derived_refresh_job WHERE loader = %s AND version = %s", (LOADER, version))
                (state,) = await cursor.fetchone()
                if state in {"completed", "superseded"}:
                    return None
                raise RefreshInProgress(f"the {version} refresh is leased by another delivery")
            return int(row[0]), token, int(generation)


async def _refresh(connection_pool: Any, logger: Any, version: str, epoch: int, token: UUID, generation: int) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    async with connection_pool.connection() as conn:
        # The pool hands out AUTOCOMMIT connections; without this every TRUNCATE would commit alone.
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute("SELECT generation, version FROM public.loader_derived_refresh_cursor WHERE loader = %s FOR UPDATE", (LOADER,))
            if await cursor.fetchone() != (generation, version):
                raise SupersededRefresh(f"a newer extraction than {version} fenced this refresh")
            await cursor.execute(
                "SELECT 1 FROM public.loader_derived_refresh_job WHERE loader = %s AND version = %s AND state = 'leased' AND lease_token = %s AND lease_epoch = %s FOR UPDATE",
                (LOADER, version, token, epoch),
            )
            if await cursor.fetchone() is None:
                raise SupersededRefresh(f"the {version} refresh lease was lost")
            for function in REFRESH_FUNCTIONS:
                started = time.perf_counter()
                await cursor.execute(_REFRESH_SQL[function])
                counts[function] = {str(key): int(rows) for key, rows in await cursor.fetchall()}
                logger.info(
                    f"🧭 Refreshed graph.{function}",
                    function=f"graph.{function}",
                    rows=counts[function],
                    duration_seconds=round(time.perf_counter() - started, 3),
                )
            await cursor.execute(
                "UPDATE public.loader_derived_refresh_job SET state = 'completed', completed_at = NOW(), lease_owner = NULL, lease_token = NULL, "
                "lease_expires_at = NULL, next_attempt_at = NULL, last_error = NULL, updated_at = NOW() "
                "WHERE loader = %s AND version = %s",
                (LOADER, version),
            )
            await cursor.execute(
                "UPDATE public.loader_extraction_latch SET refreshed_at = NOW(), updated_at = NOW() WHERE loader = %s AND version = %s",
                (LOADER, version),
            )
    return counts


async def _mark_retry(connection_pool: Any, version: str, epoch: int, token: UUID, attempt: int, error: str) -> None:
    delay = min(_MAX_BACKOFF_SECONDS, 2 ** min(attempt, 8))
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(
                "UPDATE public.loader_derived_refresh_job SET state = 'retry', next_attempt_at = NOW() + (%s * INTERVAL '1 second'), "
                "lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, last_error = %s, updated_at = NOW() "
                "WHERE loader = %s AND version = %s AND state = 'leased' AND lease_token = %s AND lease_epoch = %s",
                (delay, error[:1024], LOADER, version, token, epoch),
            )


async def refresh_derived_relations(connection_pool: Any, logger: Any, version: str) -> Mapping[str, Mapping[str, int]] | None:
    """Run the refresh pair for one completed extraction, once, and record the job.

    Returns:
        The row count per provenance (member-of) and per vertex kind (degree), or ``None`` when the
        extraction was already refreshed or a newer one superseded this pass.

    Raises:
        Exception: Whatever PostgreSQL raised, after the job is marked ``retry``; the caller must
            requeue the signal so the pass runs again.
    """
    claim = await _claim(connection_pool, version)
    if claim is None:
        logger.info("⏭️ Derived relations already refreshed for this extraction", version=version)
        return None
    epoch, token, generation = claim
    started = time.perf_counter()
    try:
        counts = await _refresh(connection_pool, logger, version, epoch, token, generation)
    except SupersededRefresh as error:
        logger.warning("⚠️ Derived refresh superseded; nothing stamped", version=version, error=str(error))
        return None
    except Exception as error:
        try:
            await _mark_retry(connection_pool, version, epoch, token, epoch, f"{type(error).__name__}: {error}")
        except Exception as mark_error:
            logger.error("❌ Could not record the failed derived refresh", version=version, error_type=type(mark_error).__name__)
        raise
    logger.info(
        "✅ Refreshed the derived graph relations",
        version=version,
        duration_seconds=round(time.perf_counter() - started, 3),
        rows=counts,
    )
    return counts
