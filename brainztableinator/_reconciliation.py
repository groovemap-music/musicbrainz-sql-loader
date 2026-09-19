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
``updated_at`` predates the extraction's ``started_at``, refuse when the delete
fraction reaches a safety cap, and skip entirely when anything was dead-lettered
this run.

Two things differ from the Discogs loader, each for a reason this loader's shape
forces.

* **The purge waits for every data type.** Discogs purges one table per
  ``data_type`` because each entity kind owns its own table. Here all four
  MusicBrainz entity kinds write into the *same* two child tables, so purging
  after ``artists`` completes would delete every relationship sourced from a
  label, release, or release group. :meth:`StaleChildRowPurge.is_latched` only
  reports ready once every entity type has signalled ``extraction_complete`` for
  the *same* ``started_at``, and the dead-letter veto is global rather than
  per-type for the same reason.

* **The natural key does the endpoint normalization, not the purge.**
  ``_record_processing.insert_relationships`` swaps the endpoints when a
  relationship arrives with ``direction: backward``, so both endpoints' messages
  upsert the *one* canonical row named by the ``relationships_natural_key``
  contract. That is what lets a single ``updated_at`` per row be sufficient: a
  relationship this run only ever saw from its backward side still refreshes the
  canonical row, and the purge, which keys on nothing but ``updated_at``, leaves
  it alone. It follows that the upserts must touch ``updated_at`` on *every*
  conflict, including the no-op ones -- see ``_persistence.py``. Without that a
  purge keyed on ``updated_at`` would delete live rows.

Why the boundary comes from the message
---------------------------------------
An earlier revision latched the boundary from ``SELECT NOW()`` at loader startup.
That is not restart-stable: the loader is a long-lived consumer, so a restart
part-way through an import moves the boundary forward past everything the run had
already written, and the next ``extraction_complete`` deletes it. A restart at 40%
would delete roughly 40% of both tables, which the delete-fraction cap does not
catch because it is well under the ceiling.

``started_at`` in the ``extraction_complete`` body is the extraction's own start,
identical across all four signals and unchanged by anything the loader does, so a
restart cannot move it. The catalog-events v1 schema makes both ``started_at`` and
``record_counts`` required on that message, so neither is an optional field this
code has to invent a default for. The cost is that ``started_at`` is the
*extractor's* clock while ``updated_at`` is written by ``NOW()`` on the database;
that is the tradeoff ``discogs-sql-loader`` already accepts, and skew in the
dangerous direction (an extractor clock ahead of the database) makes rows look
stale rather than fresh, which the cap and the record-count veto then catch.

The four-signal latch is still process memory, and that is deliberate. Losing it
to a restart can only *skip* a purge, never widen one, because the boundary no
longer depends on when this process started. A skipped purge leaves stale rows for
one run; a widened one deletes live data.

A long-lived loader sees more than one extraction. Each signal carrying a newer
``started_at`` re-latches: the accumulated signals are dropped and collection
starts again for the new boundary, so a later extraction's first signal cannot
re-fire the previous extraction's purge.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from psycopg import sql

from brainztableinator.queue_names import MUSICBRAINZ_DATA_TYPES


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


# The two child-row tables this module reconciles, as (schema, table) pairs. Both
# are written by every MusicBrainz entity kind, which is why the purge is latched
# on all of them completing rather than on one data type's signal.
RECONCILED_TABLES: tuple[tuple[str, str], ...] = (
    ("musicbrainz", "relationships"),
    ("musicbrainz", "external_links"),
)

# The column the purge keys on and the upserts refresh. `groovemap-database-schema`
# owns every DDL statement against these tables -- this loader issues none -- so the
# column is probed for rather than created, and the whole feature stays off until the
# schema owner has added it and this repository's pin has been promoted.
RECONCILIATION_COLUMN = "updated_at"

# The same 90% ceiling ``discogs-sql-loader`` refuses at: past it, a shrink that
# large is far more likely to be a resumed or partial extraction than a real
# upstream deletion, and the cheap failure is leaving stale rows for one run.
DEFAULT_MAX_DELETE_FRACTION = 0.9

_COLUMN_PRESENT = """
    SELECT count(*)
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s AND column_name = %s
"""

_COUNT_ALL = sql.SQL("SELECT count(*) FROM {table}")
_COUNT_STALE = sql.SQL("SELECT count(*) FROM {table} WHERE updated_at < %s")
_DELETE_STALE = sql.SQL("DELETE FROM {table} WHERE updated_at < %s")


def _identifier(schema: str, table: str) -> sql.Identifier:
    return sql.Identifier(schema, table)


def _qualified(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def parse_started_at(started_at: Any) -> datetime | None:
    """Return the extraction's start as an aware datetime, or ``None`` if unusable.

    A naive timestamp is read as UTC, matching how ``discogs-sql-loader`` reads the
    same field. Anything that is not an ISO-8601 string is rejected rather than
    guessed at, because the value becomes a DELETE boundary.
    """
    if not isinstance(started_at, str) or not started_at:
        return None
    try:
        parsed = datetime.fromisoformat(started_at)
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


async def reconciliation_columns_present(
    connection_pool: Any,
    logger: Any,
    tables: Sequence[tuple[str, str]] = RECONCILED_TABLES,
) -> bool:
    """Report whether every reconciled table carries the column the purge keys on.

    This loader never issues DDL: ``groovemap-database-schema`` is the only
    repository that does, and ``discogs-sql-loader`` issues none either. The column
    is being added there by ``gm-database-schema-uvs``; until that lands and this
    repository's pin is promoted, the probe answers ``False`` and the whole feature
    stays off, which is why it is a probe and not a migration.
    """
    missing: list[str] = []
    async with connection_pool.connection() as conn, conn.cursor() as cursor:
        for schema, table in tables:
            await cursor.execute(_COLUMN_PRESENT, (schema, table, RECONCILIATION_COLUMN))
            row = await cursor.fetchone()
            if not row or not row[0]:
                missing.append(_qualified(schema, table))

    if missing:
        logger.warning(
            f"⚠️ Delete-reconciliation disabled — {RECONCILIATION_COLUMN} is absent on {', '.join(missing)}; "
            f"loading continues and the purge enables itself once the schema pin carrying the column is promoted",
            missing=missing,
            column=RECONCILIATION_COLUMN,
        )
        return False

    logger.info(
        "🧭 Delete-reconciliation enabled",
        tables=[_qualified(schema, table) for schema, table in tables],
        column=RECONCILIATION_COLUMN,
    )
    return True


class StaleChildRowPurge:
    """Remove child rows no longer present upstream, under a cap and a veto."""

    def __init__(
        self,
        connection_pool: Any,
        logger: Any,
        max_delete_fraction: float = DEFAULT_MAX_DELETE_FRACTION,
        tables: Sequence[tuple[str, str]] = RECONCILED_TABLES,
        expected_data_types: Sequence[str] = tuple(MUSICBRAINZ_DATA_TYPES),
    ) -> None:
        self._connection_pool = connection_pool
        self._logger = logger
        self._max_delete_fraction = max_delete_fraction
        self._tables = tuple(tables)
        self._expected_data_types = frozenset(expected_data_types)
        self._boundary: datetime | None = None
        self._signals: dict[str, int | None] = {}
        self._dead_lettered: set[str] = set()
        self._reconciled_boundary: datetime | None = None

    @property
    def boundary(self) -> datetime | None:
        """The ``started_at`` the collected signals agree on, if any have arrived."""
        return self._boundary

    @property
    def signalled(self) -> frozenset[str]:
        """The data types that have signalled completion for the current boundary."""
        return frozenset(self._signals)

    @property
    def dead_lettered(self) -> frozenset[str]:
        """The data types that dead-lettered at least one delivery this run."""
        return frozenset(self._dead_lettered)

    def reset(self) -> None:
        """Forget the boundary, the collected signals, and the dead-letter marks."""
        self._boundary = None
        self._signals.clear()
        self._dead_lettered.clear()

    def record_dead_letter(self, data_type: str) -> None:
        """Mark that a delivery for ``data_type`` was rejected to the dead-letter queue.

        A dead-lettered record is one whose row was never upserted even though the
        record is still present upstream, so its ``updated_at`` was never refreshed
        and it would read as stale. Vetoing the purge is what stops a poison message
        from deleting a still-current record beyond the dead-letter queue.
        """
        self._dead_lettered.add(data_type)

    def record_completion(self, data_type: str, message: Mapping[str, Any]) -> None:
        """Record one ``extraction_complete`` signal against the boundary it names.

        A signal carrying a newer ``started_at`` than the one being collected starts a
        fresh collection, which is what lets a long-lived loader reconcile a second
        extraction instead of re-firing the first one's purge. A signal carrying an
        older ``started_at`` is a late or redelivered straggler from a finished
        extraction and is ignored, so it cannot drag the boundary backwards.
        """
        started_at = parse_started_at(message.get("started_at"))
        if started_at is None:
            self._logger.warning(
                "⚠️ extraction_complete carried no usable started_at — delete-reconciliation cannot latch",
                data_type=data_type,
                started_at=message.get("started_at"),
            )
            self._boundary = None
            self._signals.clear()
            return

        if self._boundary is not None and started_at < self._boundary:
            self._logger.info(
                "⏮️ Ignoring an extraction_complete older than the boundary being collected",
                data_type=data_type,
                started_at=started_at.isoformat(),
                boundary=self._boundary.isoformat(),
            )
            return

        if self._boundary is None or started_at > self._boundary:
            if self._signals:
                self._logger.info(
                    "🔁 Re-latching delete-reconciliation on a newer extraction",
                    previous_boundary=self._boundary.isoformat() if self._boundary else None,
                    boundary=started_at.isoformat(),
                    dropped=sorted(self._signals),
                )
            self._boundary = started_at
            self._signals.clear()

        record_counts = message.get("record_counts")
        count = record_counts.get(data_type) if isinstance(record_counts, dict) else None
        self._signals[data_type] = count if isinstance(count, int) else None

    def is_latched(self) -> bool:
        """Report whether every entity type has signalled for one shared boundary.

        Both child tables are fed by all four entity kinds, so a purge run after a
        subset completes would delete the rows the other kinds have not sent yet.
        """
        return self._boundary is not None and self._expected_data_types <= self._signals.keys()

    def pending_data_types(self) -> list[str]:
        """The entity types whose ``extraction_complete`` is still outstanding."""
        return sorted(self._expected_data_types - self._signals.keys())

    def _veto_reason(self) -> str | None:
        """Return why the purge must not run, or ``None`` when it may."""
        if not self.is_latched():
            return f"not every entity type has signalled: still waiting on {self.pending_data_types()}"
        if self._reconciled_boundary is not None and self._reconciled_boundary == self._boundary:
            return "this extraction has already been reconciled"
        if self._dead_lettered:
            return f"deliveries were dead-lettered this run: {sorted(self._dead_lettered)}"
        empty = sorted(data_type for data_type, count in self._signals.items() if not count)
        if empty:
            return f"the extractor reported no records for {empty} (resumed extraction?)"
        return None

    async def purge(self) -> dict[str, int]:
        """Delete stale child rows and return the per-table delete counts.

        The whole reconciliation runs in one transaction, so either every table's
        deletion commits or none does, and re-running it after a successful pass
        deletes nothing because no row older than the boundary survives.
        """
        veto = self._veto_reason()
        if veto is not None:
            self._logger.warning(f"⚠️ Skipping MusicBrainz delete-reconciliation — {veto}", reason=veto)
            # Mirrors discogs-sql-loader's reset_dlq_nacks: the marks belong to the
            # extraction that has just concluded, so they must not veto the next one.
            if self.is_latched():
                self._dead_lettered.clear()
            return {}

        boundary = self._boundary
        deleted: dict[str, int] = {}
        try:
            async with self._connection_pool.connection() as conn:
                await conn.set_autocommit(False)
                async with conn.transaction(), conn.cursor() as cursor:
                    for schema, table in self._tables:
                        deleted[_qualified(schema, table)] = await self._purge_table(cursor, schema, table, boundary)
        except Exception as exc:
            self._logger.error(
                "❌ MusicBrainz delete-reconciliation failed",
                error=str(exc),
            )
            raise

        self._reconciled_boundary = boundary
        self._dead_lettered.clear()
        self._logger.info(
            "🧹 MusicBrainz delete-reconciliation complete",
            boundary=boundary.isoformat() if boundary else None,
            deleted=deleted,
            total_deleted=sum(deleted.values()),
        )
        return deleted

    async def _purge_table(self, cursor: Any, schema: str, table: str, boundary: Any) -> int:
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
            (boundary,),
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
            (boundary,),
        )
        deleted_count = int(cursor.rowcount)
        self._logger.info(
            f"🧹 Reconciled {deleted_count} stale {qualified} rows (not refreshed since the extraction started)",
            table=qualified,
            deleted=deleted_count,
            total=total_count,
        )
        return deleted_count
