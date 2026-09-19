"""Delete-reconciliation for the MusicBrainz child-row tables.

``musicbrainz.relationships`` and ``musicbrainz.external_links`` were append-only:
every message upserts the child rows its record still carries, and a row whose
upstream relationship or link was removed simply stops being re-sent. Nothing
ever deleted it, so the relational graph drifted permanently from
``musicbrainz-graph-enricher``'s, which prunes by source. The coverage spike
``gm-database-schema-9c8.2-cypher-coverage.md`` ("Two structural gaps in
musicbrainz.relationships") is where that divergence was measured.

The reconciliation here is the same shape as ``discogs-sql-loader``'s
``purge_stale_rows`` (``tableinator/record_persistence.py`` lines 26-112, latched
by ``tableinator/tableinator.py`` lines 694-765): delete the rows whose
``updated_at`` predates the run start, refuse when the delete fraction reaches a
safety cap, and skip entirely when anything was dead-lettered this run. Three
things differ, each for a reason this loader's shape forces:

* **The purge waits for every data type.** Discogs purges one table per
  ``data_type`` because each entity kind owns its own table. Here all four
  MusicBrainz entity kinds write into the *same* two child tables, so purging
  after ``artists`` completes would delete every relationship sourced from a
  label, release, or release group. ``latched_for`` therefore only reports ready
  once every entity type has signalled ``extraction_complete``, and the
  dead-letter veto is global rather than per-type for the same reason.

* **Run start comes from the run, not from the message.** Discogs reads
  ``started_at`` out of the ``extraction_complete`` body, which is the
  *extractor's* wall clock on another host, compared against an ``updated_at``
  written by ``NOW()`` on the database. :meth:`StaleChildRowPurge.latch_run_start`
  reads ``NOW()`` from the same database clock the upserts stamp rows with, so no
  host clock skew can make a freshly refreshed row look stale. The cost is that a
  loader restart mid-import moves the run start forward and makes everything
  written before the restart look stale; the delete-fraction cap is what stops
  that from emptying the tables, exactly as it stops a resumed extraction from
  doing so in the Discogs loader.

* **The natural key does the endpoint normalization, not the purge.**
  ``_record_processing.insert_relationships`` swaps the endpoints when a
  relationship arrives with ``direction: backward``, so both endpoints' messages
  upsert the *one* canonical row named by the ``relationships_natural_key``
  contract. That is what lets a single ``updated_at`` per row be sufficient: a
  relationship this run only ever saw from its backward side still refreshes the
  canonical row, and the purge, which keys on nothing but ``updated_at``, leaves
  it alone. It follows that the upserts must touch ``updated_at`` on *every*
  conflict, including the no-op ones -- see ``_persistence.py``, where both
  ``ON CONFLICT ... DO UPDATE`` clauses now set it. Without that a purge keyed on
  ``updated_at`` would delete live rows.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from psycopg import sql


if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from datetime import datetime


# The two child-row tables this module reconciles, as (schema, table) pairs. Both
# are written by every MusicBrainz entity kind, which is why the purge is latched
# on all of them completing rather than on one data type's signal.
RECONCILED_TABLES: tuple[tuple[str, str], ...] = (
    ("musicbrainz", "relationships"),
    ("musicbrainz", "external_links"),
)

# The same 90% ceiling ``discogs-sql-loader`` refuses at: past it, a shrink that
# large is far more likely to be a resumed or partial extraction than a real
# upstream deletion, and the cheap failure is leaving stale rows for one run.
DEFAULT_MAX_DELETE_FRACTION = 0.9

# ``musicbrainz.relationships`` and ``musicbrainz.external_links`` are declared by
# ``groovemap-database-schema`` with ``created_at`` only -- unlike every entity
# table beside them, which carries ``updated_at``. A purge keyed on ``created_at``
# would delete live rows, because a row that has been re-upserted unchanged keeps
# its original ``created_at``, so the column this purge needs has to exist before
# it can run at all. This is the expand half of the "expand, migrate consumers,
# then contract" rollout the persistence contract prescribes
# (``contracts/persistence/v1/compatibility.json``), which also declares additive
# changes compatible. The statement is idempotent and spells the column exactly as
# the schema owner will declare it, so database-schema absorbing it is a no-op
# here; until it does, this loader carries it. ``NOW()`` is STABLE rather than
# volatile, so PostgreSQL takes the fast ADD COLUMN path and does not rewrite the
# table. No index is added: the purge counts the whole table anyway, and building
# one at startup would hold a lock over a table this large.
_ADD_UPDATED_AT = sql.SQL("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")

_COUNT_ALL = sql.SQL("SELECT count(*) FROM {table}")
_COUNT_STALE = sql.SQL("SELECT count(*) FROM {table} WHERE updated_at < %s")
_DELETE_STALE = sql.SQL("DELETE FROM {table} WHERE updated_at < %s")


def _identifier(schema: str, table: str) -> sql.Identifier:
    return sql.Identifier(schema, table)


def _qualified(schema: str, table: str) -> str:
    return f"{schema}.{table}"


async def ensure_reconciliation_columns(
    connection_pool: Any,
    logger: Any,
    tables: Sequence[tuple[str, str]] = RECONCILED_TABLES,
) -> None:
    """Add the ``updated_at`` column the purge keys on, idempotently.

    Runs once at startup, before the run start is latched, so that every row
    already in the table carries a timestamp that predates this run and a row this
    run refreshes carries a later one.
    """
    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction(), conn.cursor() as cursor:
            for schema, table in tables:
                await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
                    _ADD_UPDATED_AT.format(table=_identifier(schema, table))
                )
    logger.info(
        "🧭 Delete-reconciliation columns ensured",
        tables=[_qualified(schema, table) for schema, table in tables],
    )


class StaleChildRowPurge:
    """Remove child rows no longer present upstream, under a cap and a veto."""

    def __init__(
        self,
        connection_pool: Any,
        logger: Any,
        max_delete_fraction: float = DEFAULT_MAX_DELETE_FRACTION,
        tables: Sequence[tuple[str, str]] = RECONCILED_TABLES,
    ) -> None:
        self._connection_pool = connection_pool
        self._logger = logger
        self._max_delete_fraction = max_delete_fraction
        self._tables = tuple(tables)
        self._run_started_at: datetime | None = None
        self._dead_lettered: set[str] = set()

    @property
    def run_started_at(self) -> datetime | None:
        """The database clock reading taken when this run began, if it was taken."""
        return self._run_started_at

    @property
    def dead_lettered(self) -> frozenset[str]:
        """The data types that dead-lettered at least one delivery this run."""
        return frozenset(self._dead_lettered)

    def reset(self) -> None:
        """Forget the latched run start and dead-letter marks (test seam)."""
        self._run_started_at = None
        self._dead_lettered.clear()

    def record_dead_letter(self, data_type: str) -> None:
        """Mark that a delivery for ``data_type`` was rejected to the dead-letter queue.

        A dead-lettered record is one whose row was never upserted even though the
        record is still present upstream, so its ``updated_at`` was never refreshed
        and it would read as stale. Vetoing the purge is what stops a poison message
        from deleting a still-current record beyond the dead-letter queue.
        """
        self._dead_lettered.add(data_type)

    async def latch_run_start(self) -> datetime:
        """Read and remember the run start from the database clock."""
        async with self._connection_pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute("SELECT NOW()")
            row = await cursor.fetchone()
        if row is None:  # pragma: no cover - a server that answers NOW() with no row
            raise RuntimeError("PostgreSQL returned no row for the run-start clock read")
        started_at: datetime = row[0]
        self._run_started_at = started_at
        self._logger.info("⏱️ Latched delete-reconciliation run start", run_started_at=started_at.isoformat())
        return started_at

    def latched_for(self, completed: Iterable[str], expected: Iterable[str]) -> bool:
        """Report whether every entity type has signalled completion.

        Both child tables are fed by all four entity kinds, so a purge run after a
        subset completes would delete the rows the other kinds have not sent yet.
        """
        return set(expected) <= set(completed)

    def _veto_reason(self, processed_records: int) -> str | None:
        """Return why the purge must not run, or ``None`` when it may."""
        if self._run_started_at is None:
            return "no run start was latched"
        if processed_records == 0:
            return "no records were processed this run (resumed extraction?)"
        if self._dead_lettered:
            return f"deliveries were dead-lettered this run: {sorted(self._dead_lettered)}"
        return None

    async def purge(self, processed_records: int) -> dict[str, int]:
        """Delete stale child rows and return the per-table delete counts.

        The whole reconciliation runs in one transaction, so either every table's
        deletion commits or none does, and re-running it after a successful pass
        deletes nothing because no row older than the run start survives.
        """
        veto = self._veto_reason(processed_records)
        if veto is not None:
            self._logger.warning(f"⚠️ Skipping MusicBrainz delete-reconciliation — {veto}", reason=veto)
            return {}

        started_at = self._run_started_at
        deleted: dict[str, int] = {}
        try:
            async with self._connection_pool.connection() as conn:
                await conn.set_autocommit(False)
                async with conn.transaction(), conn.cursor() as cursor:
                    for schema, table in self._tables:
                        deleted[_qualified(schema, table)] = await self._purge_table(cursor, schema, table, started_at)
        except Exception as exc:
            self._logger.error(
                "❌ MusicBrainz delete-reconciliation failed",
                error=str(exc),
            )
            raise

        self._logger.info(
            "🧹 MusicBrainz delete-reconciliation complete",
            deleted=deleted,
            total_deleted=sum(deleted.values()),
        )
        return deleted

    async def _purge_table(self, cursor: Any, schema: str, table: str, started_at: Any) -> int:
        """Delete one table's stale rows, refusing when the delete fraction hits the cap."""
        qualified = _qualified(schema, table)
        identifier = _identifier(schema, table)

        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            _COUNT_ALL.format(table=identifier)
        )
        total_row = await cursor.fetchone()
        total_count = total_row[0] if total_row else 0
        if total_count == 0:
            self._logger.info(f"✅ No {qualified} rows to reconcile (table empty)", table=qualified)
            return 0

        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            _COUNT_STALE.format(table=identifier),
            (started_at,),
        )
        stale_row = await cursor.fetchone()
        stale_count = stale_row[0] if stale_row else 0
        if stale_count == 0:
            self._logger.info(f"✅ No stale {qualified} rows to reconcile", table=qualified)
            return 0

        delete_fraction = stale_count / total_count
        if delete_fraction >= self._max_delete_fraction:
            self._logger.error(
                f"🛡️ Refusing to reconcile {stale_count}/{total_count} {qualified} rows "
                f"({delete_fraction:.1%} of table) — exceeds safety cap, likely a resumed "
                f"or partial extraction rather than an upstream deletion",
                table=qualified,
                stale=stale_count,
                total=total_count,
                fraction=round(delete_fraction, 4),
                cap=self._max_delete_fraction,
            )
            return 0

        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            _DELETE_STALE.format(table=identifier),
            (started_at,),
        )
        deleted_count = int(cursor.rowcount)
        self._logger.info(
            f"🧹 Reconciled {deleted_count} stale {qualified} rows (not refreshed since the run started)",
            table=qualified,
            deleted=deleted_count,
            total=total_count,
        )
        return deleted_count
