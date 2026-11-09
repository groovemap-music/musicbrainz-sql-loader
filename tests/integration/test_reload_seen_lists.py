"""Real-PostgreSQL regressions for the reload seen lists and the no-op upserts.

These need a schema that declares ``loader_seen`` (database-schema ``gm-database-schema-uw6d``).
The persistence pin this repository records predates it, so against the pinned initializer every
test here skips, and the loader's startup probe keeps it in touch mode, which
``test_delete_reconciliation.py`` covers. Run them against database-schema main by installing that
checkout's ``groovemap-database-schema`` into the environment, as docs/configuration.md describes.

What they prove, with nothing stubbed below the processor:

* an unchanged reload in seen mode rewrites no entity, relationship or link row, leaves no dead
  tuple, and writes a small fraction of the WAL a touch reload writes;
* a shadow reload's comparison matches (``touch_purge = seen_purge``, ``refreshed_not_seen = 0``)
  and the touch purge still runs;
* a seen purge deletes exactly the rows the extraction stopped sending, and restarts both lists;
* every failure purges nothing: a crash that empties the lists, a list started after the
  extraction began, and an intact list that is missing most keys (the delete-fraction cap).
"""

from __future__ import annotations

import os
import uuid
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from common import AsyncPostgreSQLPool, WriteAdmission
from groovemap_schema.postgres import create_postgres_schema
from psycopg.conninfo import conninfo_to_dict

from brainztableinator._persistence import PostgreSQLMusicBrainzWriter
from brainztableinator._reconciliation import StaleChildRowPurge
from brainztableinator._record_processing import MusicBrainzRecordProcessor
from brainztableinator._seen_lists import ReloadMode, missing_seen_list_objects, resolve_reload_mode


if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DATA_TYPES = ("artists", "labels", "release-groups", "releases")
RELATIONSHIPS = "musicbrainz.relationships"
EXTERNAL_LINKS = "musicbrainz.external_links"
CHILD_TABLES = (RELATIONSHIPS, EXTERNAL_LINKS)
ENTITY_TABLES = ("musicbrainz.artists", "musicbrainz.labels", "musicbrainz.releases", "musicbrainz.release_groups")

ARTIST = str(uuid.UUID(int=0xA47157))
KEPT_TARGET = str(uuid.UUID(int=0x4EEB))
REMOVED_TARGET = str(uuid.UUID(int=0x4E0E4))
BACKWARD_TARGET = str(uuid.UUID(int=0xBAC4))
KEPT_URL = "https://example.com/kept"
REMOVED_URL = "https://example.com/removed"


class _NullObserver:
    def flush(self, entity: str) -> Any:
        del entity
        return nullcontext(None)

    def record(self, entity: str, size: int, duration_s: float, outcome: str) -> None:
        del entity, size, duration_s, outcome

    def alias_conflict(self, entity: str, identifier: str) -> None:
        del entity, identifier

    def set_outcome(self, span: Any, outcome: str) -> None:
        del span, outcome


@pytest_asyncio.fixture
async def pool() -> AsyncIterator[AsyncPostgreSQLPool]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    built = AsyncPostgreSQLPool(connection_params=conninfo_to_dict(database_url), max_retries=1)
    await built.initialize()
    try:
        failures = await create_postgres_schema(built)
        assert failures == 0, f"{failures} schema statements failed against the integration container"
        missing = await missing_seen_list_objects(built)
        if missing:
            pytest.skip(f"the schema under test has no reload seen lists ({missing[0]} and {len(missing) - 1} more are missing)")
        await _truncate(built)
        yield built
        await _truncate(built)
    finally:
        await built.close()


async def _execute(pool: AsyncPostgreSQLPool, statement: str, params: Any = None) -> list[tuple[Any, ...]]:
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        cursor = await conn.execute(statement, params)
        return await cursor.fetchall() if cursor.description else []


async def _scalar(pool: AsyncPostgreSQLPool, statement: str, params: Any = None) -> Any:
    rows = await _execute(pool, statement, params)
    return rows[0][0]


async def _truncate(pool: AsyncPostgreSQLPool) -> None:
    await _execute(pool, f"TRUNCATE {', '.join(CHILD_TABLES)}")
    await _execute(pool, f"TRUNCATE {', '.join(ENTITY_TABLES)}")
    await _execute(pool, "TRUNCATE public.identity_link_events")
    for data_type in ("relationships", "external_links"):
        await _execute(pool, "SELECT loader_seen.start('musicbrainz', %s)", (data_type,))


def _processor(mode: ReloadMode) -> MusicBrainzRecordProcessor:
    return MusicBrainzRecordProcessor(PostgreSQLMusicBrainzWriter(refresh_updated_at=True, reload_mode=mode), _NullObserver(), lambda record: record)


def _relationship(target: str, kind: str, *, direction: str | None = None, ended: bool = False) -> dict[str, Any]:
    relationship: dict[str, Any] = {"target_mbid": target, "target_type": "artist", "type": kind, "attributes": ["guitar"], "ended": ended}
    if direction is not None:
        relationship["direction"] = direction
    return relationship


def _artist(
    mbid: str, *, relations: list[dict[str, Any]], links: list[str], name: str = "Artist", dump_version: str = "20261001-001001"
) -> dict[str, Any]:
    return {
        "id": mbid,
        "mbid": mbid,
        "name": name,
        "sort_name": name,
        "mb_type": "Group",
        "dump_version": dump_version,
        "sha256": "content-hash",
        "life_span": {"begin": "1990", "end": None, "ended": False},
        "aliases": [{"name": f"{name} alias", "locale": None}],
        "tags": [{"name": "rock", "count": 3}],
        "relations": relations,
        "external_links": [{"url": url, "service": "official homepage"} for url in links],
    }


def _import(*, removed: bool, ended: bool = False, dump_version: str = "20261001-001001") -> list[dict[str, Any]]:
    """One extraction: an artist with a kept, an optional removed, and a backward relationship."""
    relations = [_relationship(KEPT_TARGET, "member of band", ended=ended), _relationship(BACKWARD_TARGET, "member of band", direction="backward")]
    links = [KEPT_URL]
    if removed:
        relations.append(_relationship(REMOVED_TARGET, "collaboration"))
        links.append(REMOVED_URL)
    # The backward relationship is also sent from its other endpoint, as a dump does.
    other_side = _artist(BACKWARD_TARGET, relations=[_relationship(ARTIST, "member of band")], links=[], name="Other", dump_version=dump_version)
    return [_artist(ARTIST, relations=relations, links=links, dump_version=dump_version), other_side]


async def _run(pool: AsyncPostgreSQLPool, processor: MusicBrainzRecordProcessor, records: list[dict[str, Any]]) -> None:
    for record in records:
        async with pool.connection() as conn:
            await conn.set_autocommit(False)
            async with conn.transaction():
                await processor.process_artist(conn, record)


async def _previous_extraction(pool: AsyncPostgreSQLPool) -> datetime:
    """Age every row and the lists, standing in for the extraction before this one; return its successor's start.

    The lists are started as the previous extraction settles, weeks before the next begins. Here
    the registry is backdated an hour instead, which is the only write to `loader_seen.lists`
    these tests make outside `loader_seen.start`, and the next extraction starts half an hour ago.
    """
    for table in (*CHILD_TABLES, *ENTITY_TABLES):
        await _execute(pool, f"UPDATE {table} SET updated_at = NOW() - INTERVAL '1 day'")  # noqa: S608 -- module constants
    for data_type in ("relationships", "external_links"):
        await _execute(pool, "SELECT loader_seen.start('musicbrainz', %s)", (data_type,))
    await _execute(pool, "UPDATE loader_seen.lists SET opened_at = NOW() - INTERVAL '1 hour' WHERE loader = 'musicbrainz'")
    started_at: datetime = await _scalar(pool, "SELECT NOW() - INTERVAL '30 minutes'")
    return started_at


def _signal(started_at: datetime) -> dict[str, Any]:
    return {
        "type": "extraction_complete",
        "version": started_at.date().isoformat(),
        "timestamp": started_at.isoformat(),
        "started_at": started_at.isoformat(),
        "record_counts": dict.fromkeys(DATA_TYPES, 2),
    }


def _latched(pool: AsyncPostgreSQLPool, started_at: datetime, mode: ReloadMode, logger: Any = None, **kwargs: Any) -> StaleChildRowPurge:
    purge = StaleChildRowPurge(pool, logger or MagicMock(), admission=WriteAdmission(4), mode=mode, **kwargs)
    for data_type in DATA_TYPES:
        purge.record_completion(data_type, _signal(started_at))
    return purge


async def _targets(pool: AsyncPostgreSQLPool) -> set[str]:
    rows = await _execute(pool, "SELECT target_mbid::text FROM musicbrainz.relationships")
    return {row[0] for row in rows}


async def _urls(pool: AsyncPostgreSQLPool) -> set[str]:
    return {row[0] for row in await _execute(pool, "SELECT url FROM musicbrainz.external_links")}


async def _row_versions(pool: AsyncPostgreSQLPool) -> dict[str, set[tuple[str, str]]]:
    """Every row's ctid and xmin, per table: a rewrite changes both."""
    versions: dict[str, set[tuple[str, str]]] = {}
    for table in (*CHILD_TABLES, *ENTITY_TABLES):
        rows = await _execute(pool, f"SELECT ctid::text, xmin::text FROM {table}")  # noqa: S608 -- module constants
        versions[table] = {(row[0], row[1]) for row in rows}
    return versions


async def _opened_at(pool: AsyncPostgreSQLPool) -> dict[str, datetime]:
    rows = await _execute(pool, "SELECT data_type, opened_at FROM loader_seen.lists WHERE loader = 'musicbrainz'")
    return {row[0]: row[1] for row in rows}


# ── The probe ───────────────────────────────────────────────────────────────


async def test_the_schema_with_the_lists_runs_the_configured_mode(pool: AsyncPostgreSQLPool) -> None:
    assert await resolve_reload_mode(pool, MagicMock(), ReloadMode.SHADOW) is ReloadMode.SHADOW
    assert await resolve_reload_mode(pool, MagicMock(), ReloadMode.SEEN) is ReloadMode.SEEN


# ── The writes ──────────────────────────────────────────────────────────────


async def test_an_unchanged_seen_reload_rewrites_no_row_and_appends_every_sighting(pool: AsyncPostgreSQLPool) -> None:
    await _run(pool, _processor(ReloadMode.TOUCH), _import(removed=False))
    started_at = await _previous_extraction(pool)
    before = await _row_versions(pool)

    # The next dump carries the same content under a new dump version.
    await _run(pool, _processor(ReloadMode.SEEN), _import(removed=False, dump_version="20261101-001001"))

    assert await _row_versions(pool) == before
    status = await _execute(
        pool,
        "SELECT data_type, (loader_seen.status('musicbrainz', data_type, %s)).seen_keys FROM unnest(ARRAY['relationships', 'external_links']) AS data_type",
        (started_at,),
    )
    # Two relationships (the backward one seen from both endpoints counts once) and one link.
    assert dict(status) == {"relationships": 2, "external_links": 1}
    sightings = await _scalar(pool, "SELECT count(*) FROM loader_seen.musicbrainz_relationships")
    assert sightings == 3


async def test_a_seen_reload_rewrites_only_what_changed(pool: AsyncPostgreSQLPool) -> None:
    await _run(pool, _processor(ReloadMode.TOUCH), _import(removed=False))
    await _previous_extraction(pool)
    before = await _row_versions(pool)

    records = _import(removed=False, ended=True)
    records[0]["name"] = "Renamed"
    await _run(pool, _processor(ReloadMode.SEEN), records)

    after = await _row_versions(pool)
    assert len(after["musicbrainz.artists"] - before["musicbrainz.artists"]) == 1
    assert len(after[RELATIONSHIPS] - before[RELATIONSHIPS]) == 1
    assert after[EXTERNAL_LINKS] == before[EXTERNAL_LINKS]
    ended = await _scalar(pool, "SELECT ended FROM musicbrainz.relationships WHERE target_mbid = %s", (KEPT_TARGET,))
    assert ended is True
    name = await _scalar(pool, "SELECT name FROM musicbrainz.artists WHERE mbid = %s", (ARTIST,))
    assert name == "Renamed"
    # The dump version is the envelope's: it reaches no entity document.
    assert await _scalar(pool, "SELECT count(*) FROM musicbrainz.artists WHERE data ? 'dump_version'") == 0


async def test_the_entity_write_records_a_link_change_and_nothing_for_an_unchanged_row(pool: AsyncPostgreSQLPool) -> None:
    processor = _processor(ReloadMode.SEEN)
    record = _import(removed=False)[0]
    record["discogs_artist_id"] = 101
    await _run(pool, processor, [record])
    await _run(pool, processor, [record])
    assert await _scalar(pool, "SELECT count(*) FROM public.identity_link_events WHERE external_id = %s", (ARTIST,)) == 0

    record["discogs_artist_id"] = 202
    await _run(pool, processor, [record])

    events = await _execute(
        pool, "SELECT event, discogs_id_before, discogs_id_after FROM public.identity_link_events WHERE external_id = %s", (ARTIST,)
    )
    assert events == [("link_changed", 101, 202)]


# ── The purges ──────────────────────────────────────────────────────────────


async def test_a_shadow_reload_matches_and_still_purges_by_updated_at(pool: AsyncPostgreSQLPool) -> None:
    await _run(pool, _processor(ReloadMode.TOUCH), _import(removed=True))
    started_at = await _previous_extraction(pool)
    await _run(pool, _processor(ReloadMode.SHADOW), _import(removed=False))
    logger = MagicMock()

    deleted = await _latched(pool, started_at, ReloadMode.SHADOW, logger).purge()

    assert deleted == {RELATIONSHIPS: 1, EXTERNAL_LINKS: 1}
    assert REMOVED_TARGET not in await _targets(pool)
    assert await _urls(pool) == {KEPT_URL}
    comparisons = {call.kwargs["table"]: call.kwargs for call in logger.info.call_args_list if "seen_list" in call.kwargs}
    assert set(comparisons) == set(CHILD_TABLES)
    for comparison in comparisons.values():
        assert comparison["matches"] is True
        assert comparison["intact"] is True
        assert comparison["refreshed_not_seen"] == 0
        assert comparison["seen_not_refreshed"] == 0
        assert comparison["touch_purge"] == comparison["seen_purge"] == 1
    assert all(opened > started_at for opened in (await _opened_at(pool)).values())


async def test_a_seen_purge_deletes_exactly_what_the_extraction_stopped_sending(pool: AsyncPostgreSQLPool) -> None:
    await _run(pool, _processor(ReloadMode.TOUCH), _import(removed=True))
    started_at = await _previous_extraction(pool)
    await _run(pool, _processor(ReloadMode.SEEN), _import(removed=False))
    kept = await _row_versions(pool)

    deleted = await _latched(pool, started_at, ReloadMode.SEEN).purge()

    assert deleted == {RELATIONSHIPS: 1, EXTERNAL_LINKS: 1}
    assert await _targets(pool) == {KEPT_TARGET, ARTIST}
    assert await _urls(pool) == {KEPT_URL}
    after = await _row_versions(pool)
    # Nothing that survived was rewritten by the reload or the purge.
    assert after[RELATIONSHIPS] <= kept[RELATIONSHIPS]
    assert all(opened > started_at for opened in (await _opened_at(pool)).values())
    assert await _scalar(pool, "SELECT count(*) FROM loader_seen.musicbrainz_relationships") == 0


async def test_a_crash_that_empties_the_lists_purges_nothing(pool: AsyncPostgreSQLPool) -> None:
    await _run(pool, _processor(ReloadMode.TOUCH), _import(removed=True))
    started_at = await _previous_extraction(pool)
    await _run(pool, _processor(ReloadMode.SEEN), _import(removed=False))
    # Crash recovery resets every unlogged relation: the lists and the registry together.
    await _execute(pool, "TRUNCATE loader_seen.lists, loader_seen.musicbrainz_relationships, loader_seen.musicbrainz_external_links")

    purge = _latched(pool, started_at, ReloadMode.SEEN)
    assert await purge.purge() == {}

    assert purge.lost_lists == ("loader_seen.musicbrainz_relationships", "loader_seen.musicbrainz_external_links")
    assert await _targets(pool) == {KEPT_TARGET, REMOVED_TARGET, ARTIST}
    assert await _urls(pool) == {KEPT_URL, REMOVED_URL}
    # Restarted, so the next extraction has intact lists.
    assert set(await _opened_at(pool)) == {"relationships", "external_links"}


async def test_a_list_started_after_the_extraction_began_purges_nothing(pool: AsyncPostgreSQLPool) -> None:
    await _run(pool, _processor(ReloadMode.TOUCH), _import(removed=True))
    started_at = await _previous_extraction(pool)
    await _run(pool, _processor(ReloadMode.SEEN), _import(removed=False))
    await _execute(pool, "SELECT loader_seen.start('musicbrainz', 'relationships')")

    purge = _latched(pool, started_at, ReloadMode.SEEN)
    assert await purge.purge() == {}

    assert purge.lost_lists == ("loader_seen.musicbrainz_relationships",)
    assert await _targets(pool) == {KEPT_TARGET, REMOVED_TARGET, ARTIST}


async def test_an_intact_list_missing_most_keys_is_refused_by_the_cap(pool: AsyncPostgreSQLPool) -> None:
    await _run(pool, _processor(ReloadMode.TOUCH), _import(removed=True))
    started_at = await _previous_extraction(pool)
    # A write path that never appended: the reload wrote nothing to the lists.
    await _run(pool, _processor(ReloadMode.TOUCH), [])

    deleted = await _latched(pool, started_at, ReloadMode.SEEN, max_delete_fraction=0.5).purge()

    assert deleted == {RELATIONSHIPS: 0, EXTERNAL_LINKS: 0}
    assert await _targets(pool) == {KEPT_TARGET, REMOVED_TARGET, ARTIST}
    assert await _urls(pool) == {KEPT_URL, REMOVED_URL}


async def test_a_dead_letter_veto_purges_nothing_and_restarts_the_lists(pool: AsyncPostgreSQLPool) -> None:
    await _run(pool, _processor(ReloadMode.TOUCH), _import(removed=True))
    started_at = await _previous_extraction(pool)
    await _run(pool, _processor(ReloadMode.SEEN), _import(removed=False))
    purge = _latched(pool, started_at, ReloadMode.SEEN)
    purge.record_dead_letter("artists")

    assert await purge.purge() == {}

    assert await _targets(pool) == {KEPT_TARGET, REMOVED_TARGET, ARTIST}
    assert all(opened > started_at for opened in (await _opened_at(pool)).values())


# ── What an unchanged reload costs ──────────────────────────────────────────


async def _measure_reload(pool: AsyncPostgreSQLPool, mode: ReloadMode, records: list[dict[str, Any]]) -> dict[str, int]:
    """Reload RECORDS after a VACUUM and a CHECKPOINT; return WAL bytes, rows rewritten, and dead tuples left.

    A CHECKPOINT first, so every page is logged whole on its first change, as it is over an
    hour-long reload under a five-minute `checkpoint_timeout`. Rows rewritten are counted from
    their row versions: HOT pruning reclaims some dead tuples in place during the reload, so
    `pgstattuple`'s count alone understates how many rows a reload rewrote.
    """
    await _execute(pool, f"VACUUM {', '.join((*CHILD_TABLES, *ENTITY_TABLES))}")
    await _execute(pool, "CHECKPOINT")
    before = await _row_versions(pool)
    start = await _scalar(pool, "SELECT pg_current_wal_insert_lsn()")
    await _run(pool, _processor(mode), records)
    wal = int(await _scalar(pool, "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), %s::pg_lsn)", (str(start),)))
    after = await _row_versions(pool)
    dead = 0
    for table in (*CHILD_TABLES, *ENTITY_TABLES):
        dead += int(await _scalar(pool, "SELECT dead_tuple_count FROM pgstattuple(%s::regclass)", (table,)))
    return {"wal_bytes": wal, "rewritten": sum(len(after[table] - before[table]) for table in after), "dead_tuples": dead}


async def test_an_unchanged_seen_reload_writes_a_fraction_of_the_touch_reload_s_wal(pool: AsyncPostgreSQLPool) -> None:
    await _execute(pool, "CREATE EXTENSION IF NOT EXISTS pgstattuple")
    targets = [str(uuid.UUID(int=0x7A000000 + index)) for index in range(20)]
    records = [
        _artist(
            str(uuid.UUID(int=0x5E000000 + index)),
            relations=[_relationship(target, "member of band") for target in targets],
            links=[f"https://example.com/{index}/{link}" for link in range(3)],
            name=f"Artist {index}",
        )
        for index in range(200)
    ]
    await _run(pool, _processor(ReloadMode.TOUCH), records)
    await _previous_extraction(pool)

    touch = await _measure_reload(pool, ReloadMode.TOUCH, records)
    shadow = await _measure_reload(pool, ReloadMode.SHADOW, records)
    seen = await _measure_reload(pool, ReloadMode.SEEN, records)
    print(f"\nunchanged reload of 200 artists, 4,000 relationships, 600 links: touch={touch} shadow={shadow} seen={seen}")

    # Touch and shadow still rewrite every child row (4,600) and no entity; seen rewrites nothing.
    assert touch["rewritten"] == shadow["rewritten"] == 4600
    assert seen["rewritten"] == 0
    assert seen["dead_tuples"] == 0
    assert seen["wal_bytes"] < touch["wal_bytes"] / 10
