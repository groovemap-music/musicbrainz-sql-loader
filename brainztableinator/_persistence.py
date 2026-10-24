"""Private PostgreSQL writes for normalized MusicBrainz records."""

from typing import Any, Protocol

from psycopg.types.json import Jsonb


# Both child-row upserts come in two spellings, chosen by whether the startup probe found
# `updated_at`. The pair is written out rather than composed at the call site: the column
# belongs to a table this loader never issues DDL against, so the statement that names it
# must be a literal a reader can check against the schema, not a string assembled at runtime.
#
# The relationship conflict target is the relationships_natural_key contract in
# database-schema; dates and attributes distinguish separate relationships. `updated_at` is
# refreshed on every conflict, including the ones that change nothing else: it is the only
# evidence that this run still saw the row, and `_reconciliation.StaleChildRowPurge` deletes
# rows that lack it. Leaving it off a no-op conflict would make a live relationship look
# stale and be purged.
_RELATIONSHIP_INSERT = (
    "INSERT INTO musicbrainz.relationships "
    "(source_mbid, source_entity_type, target_mbid, target_entity_type, relationship_type, attributes, begin_date, end_date, ended) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
    "ON CONFLICT (source_mbid, target_mbid, source_entity_type, target_entity_type, relationship_type, begin_date, end_date, attributes) "
)

RELATIONSHIP_UPSERTS: dict[bool, str] = {
    False: _RELATIONSHIP_INSERT + "DO UPDATE SET ended = EXCLUDED.ended",
    True: _RELATIONSHIP_INSERT + "DO UPDATE SET ended = EXCLUDED.ended, updated_at = NOW()",
}

# The external-link conflict target already covers every column, so the assignment to `url`
# is a no-op; it is there so the clause is valid, and the refresh beside it is what marks
# the link as still present this run.
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


# The Identity Ledger's live half. A load that changes an existing row's Discogs id records the
# change in `public.identity_link_events` in the same statement as the upsert, so the event
# commits or rolls back with the row it describes and costs no second round trip.
#
# Each upsert below is a three-part statement: `upsert` is the unchanged INSERT ... ON CONFLICT
# with a RETURNING of the id it stored, `old` reads the id the row held, and the final INSERT
# writes one event when the two differ. The parameters are the upsert's own, then the MBID
# `old` looks up, then the `source_ref`. All three see one snapshot, so `old` is the id the row
# held before this statement whatever `upsert` does. A brand-new row has no `old` row and the
# join yields nothing, so it writes no event; an unchanged id compares equal and writes none; a
# redelivered message finds its own id already stored and compares equal, which is what keeps
# a reprocess from duplicating an event. The event kind is read off the two ids, matching the
# table's `identity_link_events_link_matches_event` CHECK: no before id is an add, no after id a
# removal, anything else a change.
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
# self-modified if `upsert` ran first, and taking it in a statement of its own would add the
# round trip this design avoids. Two messages for one MBID racing in separate transactions can
# therefore record the earlier one's stored id as the later one's `before`. Each message is one
# transaction, so the one `event_at` per transaction never collides within the idempotence key.


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

    async def replace_release_media_edges(self, conn: Any, release_id: str, source: str, rows: list[tuple[str, str, str, int]]) -> None: ...


class PostgreSQLMusicBrainzWriter:
    """Execute the MusicBrainz schema's entity-specific write statements."""

    def __init__(self, refresh_updated_at: bool = False) -> None:
        # `updated_at` is not yet declared on musicbrainz.relationships or
        # musicbrainz.external_links: groovemap-database-schema owns every DDL
        # statement against them and is adding it under gm-database-schema-uvs. This
        # loader issues no DDL, so the clause that refreshes the column is emitted
        # only once a startup probe has seen it, and the service runs unchanged
        # against the currently pinned schema. See `_reconciliation`.
        self._refresh_updated_at = refresh_updated_at

    @property
    def refresh_updated_at(self) -> bool:
        """Whether child-row conflicts refresh the delete-reconciliation column."""
        return self._refresh_updated_at

    def set_refresh_updated_at(self, enabled: bool) -> None:
        """Enable the refresh once the startup probe has found the column."""
        self._refresh_updated_at = enabled

    async def upsert_artist(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None:
        params = (*values[:13], Jsonb(values[13]), Jsonb(values[14]), Jsonb(values[15]), values[16], values[0], source_ref)
        async with conn.cursor() as cursor:
            await cursor.execute(
                "WITH upsert AS ("
                "INSERT INTO musicbrainz.artists "
                "(mbid, name, sort_name, type, gender, begin_date, end_date, ended, "
                "area, begin_area, end_area, disambiguation, discogs_artist_id, aliases, tags, data, gm_item_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (mbid) DO UPDATE SET "
                "name = EXCLUDED.name, sort_name = EXCLUDED.sort_name, "
                "type = EXCLUDED.type, gender = EXCLUDED.gender, "
                "begin_date = EXCLUDED.begin_date, end_date = EXCLUDED.end_date, "
                "ended = EXCLUDED.ended, area = EXCLUDED.area, "
                "begin_area = EXCLUDED.begin_area, end_area = EXCLUDED.end_area, "
                "disambiguation = EXCLUDED.disambiguation, "
                "discogs_artist_id = EXCLUDED.discogs_artist_id, "
                "aliases = EXCLUDED.aliases, tags = EXCLUDED.tags, "
                "gm_item_id = EXCLUDED.gm_item_id, "
                "data = EXCLUDED.data, updated_at = NOW() "
                "RETURNING mbid, discogs_artist_id), "
                "old AS (SELECT discogs_artist_id FROM musicbrainz.artists WHERE mbid = %s) "
                "INSERT INTO public.identity_link_events "
                "(provider, entity_kind, external_id, event, discogs_id_before, discogs_id_after, event_at, dated_from, source_ref) "
                "SELECT 'musicbrainz', 'artist', upsert.mbid::text, "
                "CASE WHEN old.discogs_artist_id IS NULL THEN 'link_added' "
                "WHEN upsert.discogs_artist_id IS NULL THEN 'link_removed' ELSE 'link_changed' END, "
                "old.discogs_artist_id, upsert.discogs_artist_id, NOW(), 'groovemap_load', %s "
                "FROM upsert CROSS JOIN old "
                "WHERE old.discogs_artist_id IS DISTINCT FROM upsert.discogs_artist_id "
                "ON CONFLICT ON CONSTRAINT identity_link_events_idempotence_key DO NOTHING",
                params,
            )

    async def upsert_label(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None:
        params = (*values[:10], Jsonb(values[10]), values[11], values[0], source_ref)
        async with conn.cursor() as cursor:
            await cursor.execute(
                "WITH upsert AS ("
                "INSERT INTO musicbrainz.labels "
                "(mbid, name, type, label_code, begin_date, end_date, ended, area, disambiguation, discogs_label_id, data, gm_item_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (mbid) DO UPDATE SET "
                "name = EXCLUDED.name, type = EXCLUDED.type, "
                "label_code = EXCLUDED.label_code, "
                "begin_date = EXCLUDED.begin_date, end_date = EXCLUDED.end_date, "
                "ended = EXCLUDED.ended, area = EXCLUDED.area, "
                "disambiguation = EXCLUDED.disambiguation, "
                "discogs_label_id = EXCLUDED.discogs_label_id, "
                "gm_item_id = EXCLUDED.gm_item_id, "
                "data = EXCLUDED.data, updated_at = NOW() "
                "RETURNING mbid, discogs_label_id), "
                "old AS (SELECT discogs_label_id FROM musicbrainz.labels WHERE mbid = %s) "
                "INSERT INTO public.identity_link_events "
                "(provider, entity_kind, external_id, event, discogs_id_before, discogs_id_after, event_at, dated_from, source_ref) "
                "SELECT 'musicbrainz', 'label', upsert.mbid::text, "
                "CASE WHEN old.discogs_label_id IS NULL THEN 'link_added' "
                "WHEN upsert.discogs_label_id IS NULL THEN 'link_removed' ELSE 'link_changed' END, "
                "old.discogs_label_id, upsert.discogs_label_id, NOW(), 'groovemap_load', %s "
                "FROM upsert CROSS JOIN old "
                "WHERE old.discogs_label_id IS DISTINCT FROM upsert.discogs_label_id "
                "ON CONFLICT ON CONSTRAINT identity_link_events_idempotence_key DO NOTHING",
                params,
            )

    async def upsert_release(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None:
        params = (*values[:6], Jsonb(values[6]), Jsonb(values[7]), values[8], values[0], source_ref)
        async with conn.cursor() as cursor:
            await cursor.execute(
                "WITH upsert AS ("
                "INSERT INTO musicbrainz.releases "
                "(mbid, name, barcode, status, release_group_mbid, discogs_release_id, media, data, gm_item_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (mbid) DO UPDATE SET "
                "name = EXCLUDED.name, barcode = EXCLUDED.barcode, "
                "status = EXCLUDED.status, "
                "release_group_mbid = EXCLUDED.release_group_mbid, "
                "discogs_release_id = EXCLUDED.discogs_release_id, "
                "media = EXCLUDED.media, "
                "gm_item_id = EXCLUDED.gm_item_id, "
                "data = EXCLUDED.data, updated_at = NOW() "
                "RETURNING mbid, discogs_release_id), "
                "old AS (SELECT discogs_release_id FROM musicbrainz.releases WHERE mbid = %s) "
                "INSERT INTO public.identity_link_events "
                "(provider, entity_kind, external_id, event, discogs_id_before, discogs_id_after, event_at, dated_from, source_ref) "
                "SELECT 'musicbrainz', 'release', upsert.mbid::text, "
                "CASE WHEN old.discogs_release_id IS NULL THEN 'link_added' "
                "WHEN upsert.discogs_release_id IS NULL THEN 'link_removed' ELSE 'link_changed' END, "
                "old.discogs_release_id, upsert.discogs_release_id, NOW(), 'groovemap_load', %s "
                "FROM upsert CROSS JOIN old "
                "WHERE old.discogs_release_id IS DISTINCT FROM upsert.discogs_release_id "
                "ON CONFLICT ON CONSTRAINT identity_link_events_idempotence_key DO NOTHING",
                params,
            )

    async def upsert_release_group(self, conn: Any, values: tuple[Any, ...], source_ref: str) -> None:
        params = (*values[:3], Jsonb(values[3]), *values[4:7], Jsonb(values[7]), values[8], values[0], source_ref)
        async with conn.cursor() as cursor:
            await cursor.execute(
                "WITH upsert AS ("
                "INSERT INTO musicbrainz.release_groups "
                "(mbid, name, type, secondary_types, first_release_date, disambiguation, discogs_master_id, data, gm_item_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (mbid) DO UPDATE SET "
                "name = EXCLUDED.name, type = EXCLUDED.type, "
                "secondary_types = EXCLUDED.secondary_types, "
                "first_release_date = EXCLUDED.first_release_date, "
                "disambiguation = EXCLUDED.disambiguation, "
                "discogs_master_id = EXCLUDED.discogs_master_id, "
                "gm_item_id = EXCLUDED.gm_item_id, "
                "data = EXCLUDED.data, updated_at = NOW() "
                "RETURNING mbid, discogs_master_id), "
                "old AS (SELECT discogs_master_id FROM musicbrainz.release_groups WHERE mbid = %s) "
                "INSERT INTO public.identity_link_events "
                "(provider, entity_kind, external_id, event, discogs_id_before, discogs_id_after, event_at, dated_from, source_ref) "
                "SELECT 'musicbrainz', 'master', upsert.mbid::text, "
                "CASE WHEN old.discogs_master_id IS NULL THEN 'link_added' "
                "WHEN upsert.discogs_master_id IS NULL THEN 'link_removed' ELSE 'link_changed' END, "
                "old.discogs_master_id, upsert.discogs_master_id, NOW(), 'groovemap_load', %s "
                "FROM upsert CROSS JOIN old "
                "WHERE old.discogs_master_id IS DISTINCT FROM upsert.discogs_master_id "
                "ON CONFLICT ON CONSTRAINT identity_link_events_idempotence_key DO NOTHING",
                params,
            )

    async def insert_relationships(self, conn: Any, rows: list[tuple[Any, ...]]) -> None:
        params = [(*row[:5], Jsonb(row[5]), *row[6:]) for row in rows]
        async with conn.cursor() as cursor:
            await cursor.executemany(RELATIONSHIP_UPSERTS[self._refresh_updated_at], params)

    async def insert_external_links(self, conn: Any, rows: list[tuple[Any, ...]]) -> None:
        async with conn.cursor() as cursor:
            await cursor.executemany(EXTERNAL_LINK_UPSERTS[self._refresh_updated_at], rows)

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

    async def replace_release_media_edges(self, conn: Any, release_id: str, source: str, rows: list[tuple[str, str, str, int]]) -> None:
        """Replace one release's media edges for one source, leaving every other source alone.

        `source` is part of `graph.issued_on`'s primary key `(release_id, medium_id, source)`
        precisely so this prune reaches only the rows this loader wrote: the `WHERE` clause
        names both the release and the source, so a row `discogs-sql-loader` wrote under
        `source = 'discogs'` is never in range of the `DELETE`. It is the relational spelling of
        the `MATCH (r)-[stale:ISSUED_ON]->(m:Medium) WHERE stale.source = $source ... DELETE
        stale` prune brainzgraphinator runs before it writes the edges.

        The insert is a plain `INSERT`, with no conflict clause, because the prune has just
        emptied the range it writes into and `media_edge_rows` yields one row per medium. A
        duplicate here would be a derivation bug, and it should fail rather than quietly
        discard the quantity the second row carried.
        """
        async with conn.cursor() as cursor:
            await cursor.execute(
                "DELETE FROM graph.issued_on WHERE release_id = %s AND source = %s",
                (release_id, source),
            )
            if rows:
                await cursor.executemany(
                    "INSERT INTO graph.issued_on (release_id, medium_id, source, qty) VALUES (%s, %s, %s, %s)",
                    rows,
                )
