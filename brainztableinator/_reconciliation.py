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

* **The relationship key does the endpoint normalization, not the purge.**
  ``_record_processing.insert_relationships`` swaps the endpoints when a
  relationship arrives with ``direction: backward``, so both endpoints' messages
  write the *one* canonical row the ``relationships_key`` index names. That is what
  lets one sighting per row be sufficient: a relationship this run only ever saw
  from its backward side still marks the canonical row. In touch and shadow modes
  the mark is ``updated_at``, so the upserts refresh it on *every* conflict,
  including the no-op ones, and the purge deletes what lacks it. In seen mode the
  mark is the row's key in its ``loader_seen`` list, so the upserts write only what
  changed, and the purge deletes from ``loader_seen.<list>_stale``: rows older than
  the extraction *and* absent from its list. See ``_seen_lists`` and
  ``_persistence.py``.

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

What this purge assumes, and what it does not check
---------------------------------------------------
The whole mechanism rests on one premise: an ``extraction_complete`` at boundary
B means the producer re-sent the *entire* dump for that entity kind, so a row not
refreshed since B is a row upstream no longer has. Nothing here asserts that
premise, and nothing can from inside this process -- a record carries no statement
about which extraction produced it or how complete that extraction was. An
incremental extraction announcing itself with the same message would look exactly
like a full one in which almost everything was deleted.

Two guards approximate it from the other side. The per-type ``record_counts``
veto refuses a boundary whose extractor reported no records for some kind, which
is the shape a resumed extraction takes; and :data:`DEFAULT_MAX_DELETE_FRACTION`
refuses any single table shrink at or past 90%, which is the shape a partial one
takes. Both are empirical: they catch the cases seen so far, not the premise. If
the producer ever gains an incremental mode, this module needs a field on the
signal saying so, not a tighter cap.

The dead-letter veto survives through the broker and the latch
---------------------------------------------------------------
The marks here are process memory. ``brainztableinator`` also reads each entity's dead-letter
queue from the broker when the fourth signal latches, and records the outcome in
``purge_vetoed`` (see ``_purge_veto``), so a restart between a dead-letter and the signal still
vetoes. A dead letter of any kind counts, including a dead-lettered ``extraction_complete``, so a
run vetoes until its dead letters are replayed or cleared.

The dead-letter veto is not scoped to an extraction
---------------------------------------------------
:meth:`StaleChildRowPurge.record_dead_letter` records a data type, not a data type
and a boundary, and :meth:`StaleChildRowPurge._settle` clears every mark. Where
two extractions interleave -- the next dump's records already arriving while the
previous dump's fourth signal is still outstanding -- a mark belonging to the
*newer* extraction is cleared when the older one settles, and the newer boundary
starts with an empty mark set.

Scoping the marks would require attributing a delivery to an extraction, and a
record message carries nothing that does: only the completion signal names a
``started_at``. So the behaviour is recorded rather than fixed. It fails in the
permissive direction -- a purge that should have been vetoed may run -- which is
why the delete-fraction cap and the record-count veto are load-bearing and not
merely defensive, and why a producer that interleaves dumps would be a change this
module has to be told about.

``reset()`` deliberately leaves the settled boundary alone
----------------------------------------------------------
:meth:`StaleChildRowPurge.reset` clears the boundary, the collected signals, and
the marks, but not ``_settled_boundary``. That is not an omission. Settling is the
guard that makes a duplicate ``extraction_complete`` harmless; a reset that
forgot it would let a redelivered signal re-latch a settled extraction and fire
exactly the purge the veto refused. Forgetting a settlement is the one thing a
reset must not do.

Every signal is potentially a duplicate
---------------------------------------
Delivery is at-least-once, so the same ``extraction_complete`` can arrive twice at
the same boundary, and the purge must be decided by something a duplicate cannot
reset. That something is the boundary itself: an extraction is *settled* once it
has been decided, whether it purged or was vetoed, and a settled boundary is
refused outright. The dead-letter marks are cleared only as part of settling, so
a repeated signal cannot find them empty and purge the rows the veto protected.
"""

from __future__ import annotations

from contextlib import AsyncExitStack
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from common import DatabaseUnavailableError
from psycopg import sql

from brainztableinator._seen_lists import SEEN_LIST_DATA_TYPES, SEEN_LIST_LOADER, ReloadMode, seen_list_relation
from brainztableinator.queue_names import MUSICBRAINZ_DATA_TYPES


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from common import WriteAdmission


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

# The type the boundary comparison requires. A `date` or a `timestamp without time
# zone` column would satisfy a name-only probe, and then the refresh clause's
# assignment cast would silently truncate `NOW()` -- to day granularity for a `date` --
# so a row refreshed hours ago would read as older than a boundary from earlier the
# same day and be deleted while still live. The probe therefore reads `data_type` and
# requires the aware type, because the failure it prevents is silent data loss.
RECONCILIATION_COLUMN_TYPE = "timestamp with time zone"

_COLUMN_TYPE = """
    SELECT data_type
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s AND column_name = %s
"""

_COUNT_ALL = sql.SQL("SELECT count(*) FROM {table}")
_COUNT_STALE = sql.SQL("SELECT count(*) FROM {table} WHERE updated_at < %s")
_DELETE_STALE = sql.SQL("DELETE FROM {table} WHERE updated_at < %s")

# The seen-list statements, each through the function database-schema declares for it. The loader
# never TRUNCATEs or DELETEs from a list itself: `loader_seen.start` is the one way to empty one,
# because it stamps the registry in the same transaction, and a list emptied behind the registry's
# back would read as intact.
_SEEN_INTACT = "SELECT loader_seen.intact(%s, %s, %s)"
_SEEN_STATUS = "SELECT opened_at, intact, seen_keys FROM loader_seen.status(%s, %s, %s)"
_SEEN_START = "SELECT loader_seen.start(%s, %s)"
_SEEN_SHADOW = sql.SQL(
    "SELECT intact, total_rows, touch_purge, seen_purge, refreshed_not_seen, seen_not_refreshed, "
    "refreshed_not_seen_sample, seen_not_refreshed_sample FROM {function}(%s)"
)
# The purge set is computed once, into a temporary table dropped at commit, and both the cap and the
# DELETE read it, so the rows counted are the rows deleted. The function refuses a list that is not
# intact (SQLSTATE 55000), so even a caller that skipped `loader_seen.intact` cannot purge from an
# emptied list.
_SEEN_STALE_INTO = sql.SQL("CREATE TEMP TABLE {stale} ON COMMIT DROP AS SELECT id FROM {function}(%s)")
_SEEN_STALE_COUNT = sql.SQL("SELECT count(*) FROM {stale}")
_SEEN_DELETE = sql.SQL("DELETE FROM {table} WHERE id IN (SELECT id FROM {stale})")

_SHADOW_COLUMNS = (
    "intact",
    "total_rows",
    "touch_purge",
    "seen_purge",
    "refreshed_not_seen",
    "seen_not_refreshed",
    "refreshed_not_seen_sample",
    "seen_not_refreshed_sample",
)


def _seen_data_type(table: str) -> str:
    return SEEN_LIST_DATA_TYPES[table]


def _seen_function(table: str, suffix: str) -> sql.Identifier:
    return sql.Identifier("loader_seen", f"{SEEN_LIST_LOADER}_{_seen_data_type(table)}_{suffix}")


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
    wrong_type: dict[str, str] = {}
    async with connection_pool.connection() as conn, conn.cursor() as cursor:
        for schema, table in tables:
            await cursor.execute(_COLUMN_TYPE, (schema, table, RECONCILIATION_COLUMN))
            row = await cursor.fetchone()
            if not row or not row[0]:
                missing.append(_qualified(schema, table))
            elif row[0] != RECONCILIATION_COLUMN_TYPE:
                wrong_type[_qualified(schema, table)] = str(row[0])

    if missing:
        logger.warning(
            f"⚠️ Delete-reconciliation disabled — {RECONCILIATION_COLUMN} is absent on {', '.join(missing)}; "
            f"loading continues and the purge enables itself once the schema pin carrying the column is promoted",
            missing=missing,
            column=RECONCILIATION_COLUMN,
        )
        return False

    if wrong_type:
        logger.error(
            f"🛑 Delete-reconciliation disabled — {RECONCILIATION_COLUMN} is declared "
            f"{', '.join(f'{table} as {declared}' for table, declared in sorted(wrong_type.items()))}, "
            f"not {RECONCILIATION_COLUMN_TYPE}; a narrower type truncates the refresh and would "
            f"delete live rows, so loading continues with the purge off",
            wrong_type=wrong_type,
            column=RECONCILIATION_COLUMN,
            required_type=RECONCILIATION_COLUMN_TYPE,
        )
        return False

    logger.info(
        "🧭 Delete-reconciliation enabled",
        tables=[_qualified(schema, table) for schema, table in tables],
        column=RECONCILIATION_COLUMN,
        column_type=RECONCILIATION_COLUMN_TYPE,
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
        admission: WriteAdmission | None = None,
        mode: ReloadMode = ReloadMode.TOUCH,
    ) -> None:
        self._connection_pool = connection_pool
        self._mode = mode
        self._lost_lists: tuple[str, ...] = ()
        self._admission = admission
        self._logger = logger
        self._max_delete_fraction = max_delete_fraction
        self._tables = tuple(tables)
        self._expected_data_types = frozenset(expected_data_types)
        self._boundary: datetime | None = None
        self._signals: dict[str, int | None] = {}
        self._dead_lettered: set[str] = set()
        self._settled_boundary: datetime | None = None

    @property
    def mode(self) -> ReloadMode:
        """How this purge decides what is stale: touch (``updated_at``), shadow, or seen."""
        return self._mode

    @property
    def lost_lists(self) -> tuple[str, ...]:
        """The seen lists the last seen-mode decision found not intact, so it purged nothing.

        The caller records them in ``purge_vetoed``, like a dead-letter veto, so the
        consecutive-veto alert counts an extraction a crash or a late list start kept from
        purging. Kept until the next decision, so a retried recording sees them again.
        """
        return self._lost_lists

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
        """Forget the boundary, the collected signals, and the dead-letter marks.

        The settled boundary survives, deliberately: see the module docstring. A reset
        that forgot it would let a redelivered signal re-fire a settled purge.
        """
        self._boundary = None
        self._signals.clear()
        self._dead_lettered.clear()

    def record_dead_letter(self, data_type: str) -> None:
        """Mark that a delivery for ``data_type`` was rejected to the dead-letter queue.

        A dead-lettered record is one whose row was never upserted even though the
        record is still present upstream, so its ``updated_at`` was never refreshed
        and it would read as stale. Vetoing the purge is what stops a poison message
        from deleting a still-current record beyond the dead-letter queue.

        The mark names a data type and not the extraction it belongs to, which is a
        known limitation under interleaved extractions; the module docstring records
        why it cannot be scoped from here. ``brainztableinator._reject`` calls this
        before the broker is told, so a rejection cannot race a concurrent signal
        through the gap between settlement and the mark.
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

    @property
    def expected_data_types(self) -> frozenset[str]:
        """The entity types whose signals this purge waits for."""
        return self._expected_data_types

    @property
    def is_settled(self) -> bool:
        """Report whether the extraction being collected has already been decided."""
        return self._boundary is not None and self._settled_boundary == self._boundary

    @property
    def settled_boundary(self) -> datetime | None:
        """The extraction this purge has already finished with, purged or vetoed."""
        return self._settled_boundary

    def _settle(self, boundary: datetime | None) -> None:
        """Record that this extraction is finished with, whether it purged or was vetoed.

        Settling a *vetoed* boundary is what makes the veto hold under at-least-once
        delivery. The marks that vetoed it belong to the extraction that has just
        concluded, so they are cleared here -- and were they cleared without the
        boundary being recorded, a single redelivered ``extraction_complete`` at the
        same boundary would find an empty mark set, re-latch, and purge exactly the
        rows the veto existed to protect. Duplicate signals are ordinary, not
        exceptional, so the guard is the boundary and not the marks.
        """
        self._settled_boundary = boundary
        self._dead_lettered.clear()

    def _veto_reason(self) -> str | None:
        """Return why the purge must not run, or ``None`` when it may."""
        if not self.is_latched():
            return f"not every entity type has signalled: still waiting on {self.pending_data_types()}"
        if self._settled_boundary is not None and self._settled_boundary == self._boundary:
            return "this extraction has already been settled"
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

        In shadow and seen modes the same transaction restarts both seen lists once the
        extraction is decided, so the next extraction finds them intact. Shadow mode purges
        exactly as touch mode does and only logs what the seen purge would have deleted;
        seen mode purges from ``loader_seen.<list>_stale`` and purges nothing at all when
        either list is not intact.
        """
        veto = self._veto_reason()
        if veto is not None:
            self._logger.warning(f"⚠️ Skipping MusicBrainz delete-reconciliation — {veto}", reason=veto)
            # A latched extraction that is vetoed is finished with, so it settles here
            # and no later signal at that boundary can purge it. An unlatched one is
            # still being collected and must not settle. See `_settle`. A vetoed
            # extraction still restarts its lists, in a transaction of their own, so the
            # next one has intact lists; a duplicate signal for a settled one must not,
            # because the next extraction may already be appending.
            if self.is_latched():
                if not self.is_settled:
                    self._lost_lists = ()
                    if self._mode.appends:
                        await self._in_transaction(self._start_lists)
                self._settle(self._boundary)
            return {}

        boundary = self._boundary
        self._lost_lists = ()
        deleted: dict[str, int] = {}

        async def reconcile(cursor: Any) -> None:
            if self._mode is ReloadMode.SEEN:
                lost = await self._lists_not_intact(cursor, boundary)
                if lost:
                    self._lost_lists = lost
                    self._logger.error(
                        f"🛡️ Skipping MusicBrainz delete-reconciliation — seen lists not intact for this extraction: {', '.join(lost)} "
                        "(emptied by a crash, or started after the extraction began); vetoed like a dead letter",
                        lost=list(lost),
                        boundary=boundary.isoformat() if boundary else None,
                    )
                    await self._start_lists(cursor)
                    return
            for schema, table in self._tables:
                if self._mode is ReloadMode.SHADOW:
                    await self._log_shadow(cursor, table, boundary)
                if self._mode is ReloadMode.SEEN:
                    deleted[_qualified(schema, table)] = await self._purge_table_seen(cursor, schema, table, boundary)
                else:
                    deleted[_qualified(schema, table)] = await self._purge_table(cursor, schema, table, boundary)
            if self._mode is ReloadMode.SHADOW:
                await self._isolated(cursor, "restart the seen lists", self._start_lists)
            elif self._mode is ReloadMode.SEEN:
                await self._start_lists(cursor)

        try:
            await self._in_transaction(reconcile)
        except Exception as exc:
            self._logger.error(
                "❌ MusicBrainz delete-reconciliation failed",
                error=str(exc),
            )
            raise

        self._settle(boundary)
        self._logger.info(
            "🧹 MusicBrainz delete-reconciliation complete",
            boundary=boundary.isoformat() if boundary else None,
            reload_mode=self._mode.value,
            deleted=deleted,
            total_deleted=sum(deleted.values()),
        )
        return deleted

    async def _in_transaction(self, work: Any) -> None:
        """Run ``work(cursor)`` in one transaction under a write-admission slot."""
        if self._admission is None:
            raise DatabaseUnavailableError("write admission is not initialized")
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(self._admission.slot())
            conn = await stack.enter_async_context(self._connection_pool.connection())
            await conn.set_autocommit(False)
            await stack.enter_async_context(conn.transaction())
            cursor = await stack.enter_async_context(conn.cursor())
            await work(cursor)

    async def _isolated(self, cursor: Any, action: str, work: Any) -> None:
        """Run shadow-only ``work(cursor)`` under a savepoint, logging rather than raising a failure.

        Shadow mode's comparison and list restart are evidence for the switch, not part of the
        purge it keeps running, so neither may roll back the touch purge or requeue its signal.
        """
        try:
            async with cursor.connection.transaction():
                await work(cursor)
        except Exception as error:
            self._logger.error(f"❌ Shadow mode could not {action}; the touch purge is unaffected", error=str(error))

    async def _start_lists(self, cursor: Any) -> None:
        """Empty both seen lists and stamp when they started, in the caller's transaction."""
        for _schema, table in self._tables:
            await cursor.execute(_SEEN_START, (SEEN_LIST_LOADER, _seen_data_type(table)))
        self._logger.info(
            "🔄 Restarted the reload seen lists for the next extraction",
            lists=[seen_list_relation(_seen_data_type(table)) for _schema, table in self._tables],
        )

    async def _lists_not_intact(self, cursor: Any, boundary: Any) -> tuple[str, ...]:
        """Return the seen lists that do not hold every key appended since ``boundary``."""
        lost: list[str] = []
        for _schema, table in self._tables:
            await cursor.execute(_SEEN_INTACT, (SEEN_LIST_LOADER, _seen_data_type(table), boundary))
            row = await cursor.fetchone()
            if not (row and row[0]):
                lost.append(seen_list_relation(_seen_data_type(table)))
        return tuple(lost)

    async def _list_status(self, cursor: Any, table: str, boundary: Any) -> dict[str, Any]:
        """Return when one list started, whether it is intact, and its distinct keys since ``boundary``."""
        await cursor.execute(_SEEN_STATUS, (SEEN_LIST_LOADER, _seen_data_type(table), boundary))
        row = await cursor.fetchone()
        if not row:
            return {}
        opened_at, intact, seen_keys = row
        return {"opened_at": opened_at.isoformat() if opened_at else None, "intact": intact, "seen_keys": seen_keys}

    async def _log_shadow(self, cursor: Any, table: str, boundary: Any) -> None:
        """Log the shadow comparison of one table's two purge sets, before the touch purge runs.

        The switch to seen mode needs ``intact``, ``refreshed_not_seen = 0`` and
        ``touch_purge = seen_purge`` (every ``seen_not_refreshed`` row explained) on both
        lists, recorded here on a real reload, and the operator's approval.
        """

        async def compare(cursor: Any) -> None:
            await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
                _SEEN_SHADOW.format(function=_seen_function(table, "shadow")),
                (boundary,),
            )
            row = await cursor.fetchone()
            shadow = dict(zip(_SHADOW_COLUMNS, row, strict=True)) if row else {}
            for sample in ("refreshed_not_seen_sample", "seen_not_refreshed_sample"):
                if shadow.get(sample) is not None:
                    shadow[sample] = list(shadow[sample])
            status = await self._list_status(cursor, table, boundary)
            matches = bool(shadow.get("intact")) and shadow.get("refreshed_not_seen") == 0 and shadow.get("touch_purge") == shadow.get("seen_purge")
            log = self._logger.info if matches else self._logger.warning
            log(
                f"🔍 Seen-list shadow comparison for musicbrainz.{table}: "
                f"{'the seen purge matches the touch purge' if matches else 'the seen purge differs from the touch purge'}",
                table=f"musicbrainz.{table}",
                seen_list=seen_list_relation(_seen_data_type(table)),
                boundary=boundary.isoformat() if boundary else None,
                matches=matches,
                status=status,
                **shadow,
            )

        await self._isolated(cursor, f"compare the musicbrainz.{table} purge sets", compare)

    async def _purge_table_seen(self, cursor: Any, schema: str, table: str, boundary: Any) -> int:
        """Delete one table's rows the extraction did not see, refusing at the delete-fraction cap.

        MusicBrainz has no producer count per child table, so the cap is the count guard: a
        list that lost most of its keys without a crash makes most rows look stale, and the cap
        refuses that shrink.
        """
        qualified = _qualified(schema, table)
        identifier = _identifier(schema, table)
        stale = sql.Identifier(f"stale_{table}")

        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            _COUNT_ALL.format(table=identifier)
        )
        total_row = await cursor.fetchone()
        total_count = total_row[0] if total_row else 0
        status = await self._list_status(cursor, table, boundary)
        if total_count == 0:
            self._logger.info(f"✅ No {qualified} rows to reconcile (table empty)", table=qualified, status=status)
            return 0

        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            _SEEN_STALE_INTO.format(stale=stale, function=_seen_function(table, "stale")),
            (boundary,),
        )
        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            _SEEN_STALE_COUNT.format(stale=stale)
        )
        stale_row = await cursor.fetchone()
        stale_count = stale_row[0] if stale_row else 0
        if stale_count == 0:
            self._logger.info(f"✅ No stale {qualified} rows to reconcile", table=qualified, status=status)
            return 0

        delete_fraction = stale_count / total_count
        if delete_fraction >= self._max_delete_fraction:
            self._logger.error(
                f"🛡️ Refusing to reconcile {stale_count}/{total_count} {qualified} rows "
                f"({delete_fraction:.1%} of table) — exceeds safety cap, likely a resumed "
                f"or partial extraction, or a seen list that lost keys, rather than an upstream deletion",
                table=qualified,
                stale=stale_count,
                total=total_count,
                fraction=round(delete_fraction, 4),
                cap=self._max_delete_fraction,
                status=status,
            )
            return 0

        await cursor.execute(  # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
            _SEEN_DELETE.format(table=identifier, stale=stale)
        )
        deleted_count = int(cursor.rowcount)
        self._logger.info(
            f"🧹 Reconciled {deleted_count} stale {qualified} rows (older than the extraction and not in its seen list)",
            table=qualified,
            deleted=deleted_count,
            total=total_count,
            status=status,
        )
        return deleted_count

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
