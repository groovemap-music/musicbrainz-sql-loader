"""Private PostgreSQL writes for normalized MusicBrainz records."""

from dataclasses import dataclass
from typing import Any, Protocol

from psycopg.errors import DataError
from psycopg.types.json import Jsonb

from brainztableinator._seen_lists import EXTERNAL_LINK_SEEN_APPEND, RELATIONSHIP_SEEN_APPEND, ReloadMode


# The relationship conflict target is database-schema's `musicbrainz.relationships_key` unique index:
# `musicbrainz.relationship_key(...)`, one 32-byte hash over the eight values that identify a relationship
# (both endpoints and their entity types, the relationship type, the date range and the attributes), so two
# instances that differ only by dates or attributes are separate rows. PostgreSQL infers the index only when
# the expression and its arguments appear exactly as the index declares them, so this text is the schema's,
# argument order included, and so is the key the seen-mode UPDATE below matches on. The index replaced the
# eight-column `relationships_natural_key` constraint, which database-schema retires before MusicBrainz is
# enabled; while a schema still carries it, it also rejects a row the hash calls new, which only happens for
# a jsonb number written at two scales (`1` and `1.0`).
_RELATIONSHIP_KEY_COLUMNS = (
    "musicbrainz.relationship_key("
    "source_mbid, target_mbid, source_entity_type, target_entity_type, relationship_type, begin_date, end_date, attributes)"
)

_RELATIONSHIP_INSERT = (
    "INSERT INTO musicbrainz.relationships "  # noqa: S608 -- module constants only
    "(source_mbid, source_entity_type, target_mbid, target_entity_type, relationship_type, attributes, begin_date, end_date, ended) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
    f"ON CONFLICT ({_RELATIONSHIP_KEY_COLUMNS}) "
)

# Touch and shadow modes: every conflict refreshes `updated_at`, changed or not, because the touch
# purge deletes the rows that lack the refresh. Both spellings are written out rather than composed
# at the call site: the `False` one is for a schema whose probe found no `updated_at`, and the
# column belongs to a table this loader never issues DDL against.
RELATIONSHIP_UPSERTS: dict[bool, str] = {
    False: _RELATIONSHIP_INSERT + "DO UPDATE SET ended = EXCLUDED.ended",
    True: _RELATIONSHIP_INSERT + "DO UPDATE SET ended = EXCLUDED.ended, updated_at = NOW()",
}

# Seen mode: the seen list is the evidence of a sighting, so an unchanged row is not written at all.
# `ended` is the only column outside the key, so the UPDATE writes a row only when `ended` changed,
# and the INSERT adds a row only when there is none. `ON CONFLICT ... DO UPDATE ... WHERE` would skip
# the update but still lock the conflicting row, and the lock writes the tuple's `xmax`: every heap
# page dirtied and logged whole after a checkpoint. A plain UPDATE locks only the rows its WHERE keeps
# and `DO NOTHING` locks none, so an unchanged reload writes almost no WAL and no dead tuples
# (database-schema docs/architecture.md, "MusicBrainz: the no-op check"). The parameters are `ended`,
# the eight key values in `relationship_key`'s order, then `ended` again.
RELATIONSHIP_SEEN_UPDATE = (
    "UPDATE musicbrainz.relationships SET ended = %s, updated_at = NOW() "  # noqa: S608 -- module constants only
    f"WHERE {_RELATIONSHIP_KEY_COLUMNS} = musicbrainz.relationship_key("
    "%s::uuid, %s::uuid, %s::text, %s::text, %s::text, %s::text, %s::text, %s::jsonb) "
    "AND ended IS DISTINCT FROM %s::boolean"
)
RELATIONSHIP_SEEN_INSERT = _RELATIONSHIP_INSERT + "DO NOTHING"

# The external-link conflict target covers every column, so a conflict is always an unchanged row.
# In touch and shadow modes the refresh of `updated_at` is what marks the link as still present (the
# assignment to `url` is a no-op that keeps the clause valid without the column); in seen mode the
# seen list does, and the insert does nothing on conflict.
_EXTERNAL_LINK_INSERT = (
    "INSERT INTO musicbrainz.external_links "
    "(mbid, entity_type, url, service_name) "
    "VALUES (%s, %s, %s, %s) "
    "ON CONFLICT (mbid, entity_type, service_name, url) "
)

EXTERNAL_LINK_UPSERTS: dict[bool, str] = {
    False: _EXTERNAL_LINK_INSERT + "DO UPDATE SET url = EXCLUDED.url",
    True: _EXTERNAL_LINK_INSERT + "DO UPDATE SET url = EXCLUDED.url, updated_at = NOW()",
}
EXTERNAL_LINK_SEEN_INSERT = _EXTERNAL_LINK_INSERT + "DO NOTHING"


# Each entity write is one statement that skips an unchanged row without writing or locking it.
# The MusicBrainz entity tables are not purged, so nothing reads their `updated_at` as evidence that
# a reload saw the row, and rewriting an unchanged row only cost WAL, a dead tuple and, for
# `musicbrainz.releases`, a pass through the identity ledger's capture trigger. `src` holds the
# record's values once, typed as the table declares them. `updated` rewrites the row only when some
# column differs from the stored one (`jsonb` compares semantically, so key order does not count as a
# change), and `inserted` adds the row only when there is none: `ON CONFLICT DO NOTHING` takes no lock
# on the existing row, where `DO UPDATE ... WHERE` would lock, dirty and log every page it skipped.
# When the row exists and differs, `inserted` meets it (or the version `updated` just wrote) and does
# nothing, so at most one of the two returns a row. Two messages inserting the same new MBID at once
# both reach `DO NOTHING` after the first commits, and the second's values wait for the next reload;
# both come from one extraction, so they carry the same record.
#
# The Identity Ledger's live half rides on the same statement. A load that changes an existing row's
# Discogs id records the change in `public.identity_link_events`, so the event commits or rolls back
# with the row it describes and costs no second round trip. `upsert` is the row `updated` or
# `inserted` returned, `old` reads the id the row held, and the final INSERT writes one event when
# the two differ. The parameters are `src`'s, then the MBID `old` looks up, then the `source_ref`.
# Every part sees one snapshot, so `old` is the id the row held before this statement. A brand-new
# row has no `old` row and the join yields nothing, so it writes no event; an unchanged row returns
# nothing from either part and writes none; a redelivered message finds its own values stored and
# writes none, which is what keeps a reprocess from duplicating an event. The event kind is read off
# the two ids, matching the table's `identity_link_events_link_matches_event` CHECK: no before id is
# an add, no after id a removal, anything else a change.
#
# `event_at` is `now()`, the load transaction's own start, which is how `dated_from =
# 'groovemap_load'` dates an event. `ON CONFLICT ON CONSTRAINT identity_link_events_idempotence_key
# DO NOTHING` is the schema's rerun guard and a backstop here: the comparison above already
# keeps a reprocess from reaching it.
#
# `entity_kind` is the value `provider_aliases` holds for the MusicBrainz row: `master` for a
# release group, as `_record_processing.RELEASE_GROUP_ENTITY_KIND` states.
#
# `old` takes no row lock: a lock taken inside this statement could be skipped as
# self-modified if `updated` ran first, and taking it in a statement of its own would add the
# round trip this design avoids. Two messages for one MBID racing in separate transactions can
# therefore record the earlier one's stored id as the later one's `before`. Each message is one
# transaction, so the one `event_at` per transaction never collides within the idempotence key.


class DuplicateMediumError(DataError):
    """One record named the same medium twice for one release: a verdict on the record.

    A `DataError`, so the delivery classifier dead-letters it at once, exactly as the
    `UniqueViolation` the old insert raised was dead-lettered, instead of requeueing a
    record that can never succeed.
    """


@dataclass(frozen=True)
class _EntityTable:
    """One MusicBrainz entity table: its columns as the schema types them, and its Discogs link."""

    table: str
    entity_kind: str
    link_column: str
    # (column, type), `mbid` first, in the order the writer's values arrive.
    columns: tuple[tuple[str, str], ...]


def _entity_write(entity: _EntityTable) -> str:
    """Return the no-op-skipping write of one entity row, with its link-change event."""
    names = [name for name, _type in entity.columns]
    compared = [name for name in names if name != "mbid"]
    link = entity.link_column
    return (
        f"WITH src ({', '.join(names)}) AS (VALUES ({', '.join(f'%s::{type_}' for _name, type_ in entity.columns)})), "  # noqa: S608 -- module constants only
        f"old AS (SELECT {link} FROM {entity.table} WHERE mbid = %s), "
        f"updated AS (UPDATE {entity.table} AS t SET {', '.join(f'{name} = src.{name}' for name in compared)}, updated_at = NOW() "
        f"FROM src WHERE t.mbid = src.mbid "
        f"AND ({', '.join(f't.{name}' for name in compared)}) IS DISTINCT FROM ({', '.join(f'src.{name}' for name in compared)}) "
        f"RETURNING t.mbid, t.{link}), "
        f"inserted AS (INSERT INTO {entity.table} ({', '.join(names)}) SELECT {', '.join(names)} FROM src "
        f"ON CONFLICT (mbid) DO NOTHING RETURNING mbid, {link}), "
        f"upsert AS (SELECT mbid, {link} FROM updated UNION ALL SELECT mbid, {link} FROM inserted) "
        "INSERT INTO public.identity_link_events "
        "(provider, entity_kind, external_id, event, discogs_id_before, discogs_id_after, event_at, dated_from, source_ref) "
        f"SELECT 'musicbrainz', '{entity.entity_kind}', upsert.mbid::text, "
        f"CASE WHEN old.{link} IS NULL THEN 'link_added' "
        f"WHEN upsert.{link} IS NULL THEN 'link_removed' ELSE 'link_changed' END, "
        f"old.{link}, upsert.{link}, NOW(), 'groovemap_load', %s "
        "FROM upsert CROSS JOIN old "
        f"WHERE old.{link} IS DISTINCT FROM upsert.{link} "
        "ON CONFLICT ON CONSTRAINT identity_link_events_idempotence_key DO NOTHING"
    )


ARTIST_WRITE = _entity_write(
    _EntityTable(
        "musicbrainz.artists",
        "artist",
        "discogs_artist_id",
        (
            ("mbid", "uuid"),
            ("name", "text"),
            ("sort_name", "text"),
            ("type", "text"),
            ("gender", "text"),
            ("begin_date", "text"),
            ("end_date", "text"),
            ("ended", "boolean"),
            ("area", "text"),
            ("begin_area", "text"),
            ("end_area", "text"),
            ("disambiguation", "text"),
            ("discogs_artist_id", "bigint"),
            ("aliases", "jsonb"),
            ("tags", "jsonb"),
            ("data", "jsonb"),
            ("gm_item_id", "uuid"),
        ),
    )
)

LABEL_WRITE = _entity_write(
    _EntityTable(
        "musicbrainz.labels",
        "label",
        "discogs_label_id",
        (
            ("mbid", "uuid"),
            ("name", "text"),
            ("type", "text"),
            ("label_code", "integer"),
            ("begin_date", "text"),
            ("end_date", "text"),
            ("ended", "boolean"),
            ("area", "text"),
            ("disambiguation", "text"),
            ("discogs_label_id", "bigint"),
            ("data", "jsonb"),
            ("gm_item_id", "uuid"),
        ),
    )
)

RELEASE_WRITE = _entity_write(
    _EntityTable(
        "musicbrainz.releases",
        "release",
        "discogs_release_id",
        (
            ("mbid", "uuid"),
            ("name", "text"),
            ("barcode", "text"),
            ("status", "text"),
            ("release_group_mbid", "uuid"),
            ("discogs_release_id", "bigint"),
            ("media", "jsonb"),
            ("data", "jsonb"),
            ("gm_item_id", "uuid"),
        ),
    )
)

RELEASE_GROUP_WRITE = _entity_write(
    _EntityTable(
        "musicbrainz.release_groups",
        "master",
        "discogs_master_id",
        (
            ("mbid", "uuid"),
            ("name", "text"),
            ("type", "text"),
            ("secondary_types", "jsonb"),
            ("first_release_date", "text"),
            ("disambiguation", "text"),
            ("discogs_master_id", "bigint"),
            ("data", "jsonb"),
            ("gm_item_id", "uuid"),
        ),
    )
)


class MusicBrainzWriter(Protocol):
    """Database operations required by MusicBrainz record processing."""

    async def upsert_artist(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None: ...

    async def upsert_label(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None: ...

    async def upsert_release(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None: ...

    async def upsert_release_group(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None: ...

    async def insert_relationships(self, conn: Any, rows: list[tuple[Any, ...]]) -> None: ...

    async def insert_external_links(self, conn: Any, rows: list[tuple[Any, ...]]) -> None: ...

    async def upsert_media_families(self, conn: Any, names: list[str]) -> None: ...

    async def upsert_media(self, conn: Any, rows: list[tuple[str, str, str]]) -> None: ...

    async def lock_release_media(self, conn: Any, release_id: str, source: str) -> None: ...

    async def replace_release_media_edges(self, conn: Any, release_id: str, source: str, rows: list[tuple[str, str, str, int]]) -> None: ...


class PostgreSQLMusicBrainzWriter:
    """Execute the MusicBrainz schema's entity-specific write statements."""

    def __init__(self, refresh_updated_at: bool = False, reload_mode: ReloadMode = ReloadMode.TOUCH) -> None:
        # `updated_at` on musicbrainz.relationships and musicbrainz.external_links, and the
        # `loader_seen` lists, belong to groovemap-database-schema, which owns every DDL
        # statement against them. This loader issues no DDL, so the clauses that name them are
        # emitted only once a startup probe has seen them, and the service runs unchanged
        # against an older schema. See `_reconciliation` and `_seen_lists`.
        self._refresh_updated_at = refresh_updated_at
        self._reload_mode = reload_mode

    @property
    def refresh_updated_at(self) -> bool:
        """Whether child-row conflicts refresh the delete-reconciliation column."""
        return self._refresh_updated_at

    def set_refresh_updated_at(self, enabled: bool) -> None:
        """Enable the refresh once the startup probe has found the column."""
        self._refresh_updated_at = enabled

    @property
    def reload_mode(self) -> ReloadMode:
        """How child-row writes prove a row is still upstream: touch, shadow or seen."""
        return self._reload_mode

    def set_reload_mode(self, mode: ReloadMode) -> None:
        """Set the mode the startup probe resolved; `touch` unless the schema carries the seen lists."""
        self._reload_mode = mode

    async def upsert_artist(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None:
        params = (*values[:13], Jsonb(values[13]), Jsonb(values[14]), Jsonb(values[15]), values[16], values[0], source_ref)
        async with conn.cursor() as cursor:
            await cursor.execute(ARTIST_WRITE, params)

    async def upsert_label(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None:
        params = (*values[:10], Jsonb(values[10]), values[11], values[0], source_ref)
        async with conn.cursor() as cursor:
            await cursor.execute(LABEL_WRITE, params)

    async def upsert_release(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None:
        params = (*values[:6], Jsonb(values[6]), Jsonb(values[7]), values[8], values[0], source_ref)
        async with conn.cursor() as cursor:
            await cursor.execute(RELEASE_WRITE, params)

    async def upsert_release_group(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None:
        params = (*values[:3], Jsonb(values[3]), *values[4:7], Jsonb(values[7]), values[8], values[0], source_ref)
        async with conn.cursor() as cursor:
            await cursor.execute(RELEASE_GROUP_WRITE, params)

    async def insert_relationships(self, conn: Any, rows: list[tuple[Any, ...]]) -> None:
        """Write one message's relationships, then append every one of them to the seen list.

        Each row is `(source_mbid, source_entity_type, target_mbid, target_entity_type,
        relationship_type, attributes, begin_date, end_date, ended)`, already normalized to its
        canonical direction. The append covers every row, changed or not: in shadow and seen
        modes the list is what the purge reads as this extraction's sighting of the row.
        """
        params = [(*row[:5], Jsonb(row[5]), *row[6:]) for row in rows]
        async with conn.cursor() as cursor:
            if self._reload_mode.touches:
                await cursor.executemany(RELATIONSHIP_UPSERTS[self._refresh_updated_at], params)
            else:
                await cursor.executemany(
                    RELATIONSHIP_SEEN_UPDATE,
                    [
                        (ended, source, target, source_type, target_type, kind, begin, end, attributes, ended)
                        for (source, source_type, target, target_type, kind, attributes, begin, end, ended) in params
                    ],
                )
                await cursor.executemany(RELATIONSHIP_SEEN_INSERT, params)
            if self._reload_mode.appends:
                await cursor.execute(
                    RELATIONSHIP_SEEN_APPEND,
                    (
                        [row[0] for row in params],
                        [row[2] for row in params],
                        [row[1] for row in params],
                        [row[3] for row in params],
                        [row[4] for row in params],
                        [row[6] for row in params],
                        [row[7] for row in params],
                        [row[5] for row in params],
                    ),
                )

    async def insert_external_links(self, conn: Any, rows: list[tuple[Any, ...]]) -> None:
        """Write one message's external links, then append every one of them to the seen list.

        Each row is `(mbid, entity_type, url, service_name)`. The key function takes the link's
        unique key in its own order, `(mbid, entity_type, service_name, url)`.
        """
        async with conn.cursor() as cursor:
            if self._reload_mode.touches:
                await cursor.executemany(EXTERNAL_LINK_UPSERTS[self._refresh_updated_at], rows)
            else:
                await cursor.executemany(EXTERNAL_LINK_SEEN_INSERT, rows)
            if self._reload_mode.appends:
                await cursor.execute(
                    EXTERNAL_LINK_SEEN_APPEND,
                    ([row[0] for row in rows], [row[1] for row in rows], [row[3] for row in rows], [row[2] for row in rows]),
                )

    # ── The shared graph relations ───────────────────────────────────────────
    # Everything below writes into the `graph` schema rather than `musicbrainz`,
    # and every statement in it is shared with `discogs-sql-loader`. The three
    # relations are recorded in `contracts/persistence/v1/compatibility.json`
    # with both loaders as their owner, which is what makes the conflict clauses
    # here part of the contract rather than defensive habit.

    async def upsert_media_families(self, conn: Any, names: list[str]) -> None:
        """Add any media families this release names to the shared vocabulary.

        `graph.media_family` is keyed on `name` and both SQL loaders write it, so the row
        already there wins. That mirrors brainzgraphinator's `MERGE (f:MediaFamily {name:
        item.family})`, which never sets a property on an existing node.
        """
        if not names:
            return
        async with conn.cursor() as cursor:
            await cursor.executemany(
                "INSERT INTO graph.media_family (name) VALUES (%s) ON CONFLICT DO NOTHING",
                [(name,) for name in names],
            )

    async def upsert_media(self, conn: Any, rows: list[tuple[str, str, str]]) -> None:
        """Add any media this release names to the shared vocabulary.

        `ON CONFLICT DO NOTHING` rather than `DO UPDATE` is the point: brainzgraphinator writes
        a medium's family and label under `MERGE (m:Medium {id: item.medium}) ON CREATE SET
        m.family = item.family, m.label = item.label`, so the pass that creates the medium is
        the only one that sets them and a later pass leaves the existing row alone. Writing the
        columns on conflict would let this loader overwrite what `discogs-sql-loader` wrote.
        """
        if not rows:
            return
        async with conn.cursor() as cursor:
            await cursor.executemany(
                "INSERT INTO graph.medium (medium_id, family, label) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                rows,
            )

    async def lock_release_media(self, conn: Any, release_id: str, source: str) -> None:
        """Serialize writers of one native release's `graph.issued_on` range until commit.

        Two MusicBrainz releases can name one Discogs release, so two messages in two
        transactions can prune and insert the same `(release_id, source)` range. Neither
        sees the other's uncommitted rows, both delete nothing, and the second insert hits
        `issued_on_pkey`. A transaction-scoped advisory lock keyed on that range makes the
        second writer wait for the first to commit, then prune what it wrote and insert its own,
        so the last committer's edges win. The lock is released by commit or rollback.

        Callers take it before any other write in the transaction. Every writer of the range
        then acquires it first, so waiting on it can never close a cycle with row locks held
        by another writer of the same range. One message holds at most one such lock, because
        one transaction processes one release; taking it again is a no-op.
        """
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"graph.issued_on:{source}:{release_id}",),
            )

    async def replace_release_media_edges(self, conn: Any, release_id: str, source: str, rows: list[tuple[str, str, str, int]]) -> None:
        """Replace one release's media edges for one source, leaving every other source alone.

        `source` is part of `graph.issued_on`'s primary key `(release_id, medium_id, source)`
        precisely so this prune reaches only the rows this loader wrote: the `WHERE` clause
        names both the release and the source, so a row `discogs-sql-loader` wrote under
        `source = 'discogs'` is never in range of the `DELETE`. It is the relational spelling of
        the `MATCH (r)-[stale:ISSUED_ON]->(m:Medium) WHERE stale.source = $source ... DELETE
        stale` prune brainzgraphinator runs before it writes the edges.

        **Only the difference is written.** The stored `(medium_id, qty)` pairs for the range are
        read (one primary-key range search, after the lock) and compared with the incoming set:
        an edge that is already stored with the same quantity is not touched, a stored edge the
        release no longer names is deleted, a new edge is inserted, and an edge whose quantity
        changed is updated. A reload of an unchanged release therefore writes no edge row, so it
        produces no WAL, dead tuples or index churn, and the range ends with exactly the incoming
        edge set either way. The advisory lock makes the read-compare-write safe against another
        writer of the same range.

        The insert is a plain `INSERT`, with no conflict clause: a medium in `rows` that is not
        stored is new, and `media_edge_rows` yields one row per medium. A duplicate medium in
        `rows` is a derivation bug, and it fails with `DuplicateMediumError` rather than quietly discarding
        the quantity the second row carried.
        """
        wanted: dict[str, int] = {}
        for _release_id, medium_id, _source, qty in rows:
            if medium_id in wanted:
                raise DuplicateMediumError(f"duplicate medium {medium_id!r} in the edges of release {release_id!r}")
            wanted[medium_id] = qty
        await self.lock_release_media(conn, release_id, source)
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT medium_id, qty FROM graph.issued_on WHERE release_id = %s AND source = %s",
                (release_id, source),
            )
            stored = dict(await cursor.fetchall())
            removed = [(release_id, source, medium_id) for medium_id in stored if medium_id not in wanted]
            changed = [(qty, release_id, medium_id, source) for medium_id, qty in wanted.items() if medium_id in stored and stored[medium_id] != qty]
            added = [row for row in rows if row[1] not in stored]
            if removed:
                await cursor.executemany(
                    "DELETE FROM graph.issued_on WHERE release_id = %s AND source = %s AND medium_id = %s",
                    removed,
                )
            if changed:
                await cursor.executemany(
                    "UPDATE graph.issued_on SET qty = %s WHERE release_id = %s AND medium_id = %s AND source = %s",
                    changed,
                )
            if added:
                await cursor.executemany(
                    "INSERT INTO graph.issued_on (release_id, medium_id, source, qty) VALUES (%s, %s, %s, %s)",
                    added,
                )
