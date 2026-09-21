"""Cross-store parity: this loader's relational edges against the enricher's Neo4j graph.

Phase 4 retires ``musicbrainz-graph-enricher`` only if the edges this loader writes
into PostgreSQL are the edges the enricher writes into Neo4j. That is a claim about
two running systems, so this suite proves it by running both: the *same* fixture
events go through ``brainztableinator``'s real message handler against a disposable
PostgreSQL and through ``brainzgraphinator``'s real message handler against a
disposable Neo4j, and the two graphs are then read back and compared per edge label.

How the enricher is driven
--------------------------
By its own delivery handler, not by a reimplementation of it. ``on_artist_message``
and its three siblings are patched onto a live driver exactly as the enricher's own
``tests/integration/test_real_neo4j_delivery.py`` does, so the comparison measures
the code that ships rather than a summary of it. The enricher is installed by
``scripts/test-parity.sh`` with ``uv pip install --no-deps`` and is deliberately not
declared in ``pyproject.toml``: it pins its own ``groovemap-runtime`` revision, which
is a hard URL conflict with the one this repository's persistence contract pins.
``--no-deps`` keeps *this* repository's runtime in place, which is what parity wants
anyway — both sides must derive media from one ``common.media``, or the comparison
would be measuring a runtime skew rather than the loader. ``uv sync`` removes the
package again, so nothing leaks into another lane, and
:func:`test_the_pinned_enricher_is_the_one_under_comparison` fails rather than
silently comparing against whatever happens to be installed.

Both stores are pre-seeded with the Discogs half of the catalog — the ``:Artist``,
``:Release``, ``:Label`` and ``:Master`` nodes and the one ``graph.member_of`` row —
because both services only ever *enrich* a catalog another loader wrote. Neither
side invents an entity vertex, so an event naming a Discogs id the catalog does not
hold is one of the divergences below rather than a new node.

What is compared
----------------
Three families of edge, read out of both stores and then grouped by edge label:

* the MusicBrainz artist-to-artist relations, whose label comes from
  ``graph.mb_relationship_type`` — the schema function that renders the enricher's
  own ``MB_RELATIONSHIP_MAP`` — together with the Discogs-provenance ``MEMBER_OF``
  they share a label with;
* ``ISSUED_ON``, the shared release-to-medium edge both loaders write; and
* ``IN_FAMILY``, the medium-to-family edge derived from the media vocabulary.

A tuple is ``(source, target, mapped relationship_type, *properties)`` in both
stores, so equality is set equality over identical shapes. Node properties are out
of scope: ``mb_updated_at`` is ``datetime.now()`` on the enricher's side and would
make every comparison non-deterministic.

The expected-differences registry
---------------------------------
:data:`EXPECTED_DIFFERENCES` is a plain mapping keyed by edge label. It is held in
both directions, which is the point: an undeclared divergence fails and names the
family it came from, and a *declared* divergence that stops materialising fails too,
because a registry nobody can retire is a registry nobody reads. Each entry lists
one :class:`Cause` per reason, so a label that diverges for three unrelated reasons
says so instead of presenting one opaque tuple set.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final
from unittest.mock import patch

import pytest
import pytest_asyncio
import structlog
from common import AsyncPostgreSQLPool, Settlement
from groovemap_schema.postgres import create_postgres_schema
from orjson import dumps
from psycopg.conninfo import conninfo_to_dict

import brainztableinator.brainztableinator as loader
from brainztableinator._reconciliation import StaleChildRowPurge, reconciliation_columns_present


if TYPE_CHECKING:
    from collections.abc import AsyncIterator


ENRICHER_DISTRIBUTION: Final = "groovemap-musicbrainz-graph-enricher"

# The enricher is installed by `scripts/test-parity.sh`, not declared in
# `pyproject.toml`, so every other lane collects this module without it and skips
# here rather than erroring during collection.
enricher = pytest.importorskip(
    "brainzgraphinator.brainzgraphinator",
    reason=f"{ENRICHER_DISTRIBUTION} is installed by the parity lane; run `just test-parity`",
)

pytestmark = [pytest.mark.integration, pytest.mark.parity]

logger = structlog.get_logger(__name__)

# The sentinel standing in for `graph.mb_relationship_type` returning NULL. A raw
# MusicBrainz relation the enricher's map does not hold has no edge label at all, so
# it cannot key the registry under one; it keys under this instead.
UNMAPPED: Final = "<unmapped>"

DATA_TYPES: Final = ("artists", "labels", "release-groups", "releases")

RELATIONSHIPS: Final = "musicbrainz.relationships"
EXTERNAL_LINKS: Final = "musicbrainz.external_links"


# ── The catalog both stores are seeded with ──────────────────────────────────
# Discogs ids, as text, because that is how the graph keys every vertex on both
# sides: `graph.release.release_id` is `public.releases.data_id` as text and the
# enricher's `MATCH (r:Release {id: $discogs_id})` binds the same string.
ARTIST_IDS: Final = ("9001", "9002", "9003", "9004")
RELEASE_IDS: Final = ("9101", "9102", "9103")
UNCATALOGUED_RELEASE_ID: Final = "9104"
LABEL_ID: Final = "9201"
MASTER_ID: Final = "9301"


def _mbid(marker: int) -> str:
    return str(uuid.UUID(int=marker))


ARTIST_MBIDS: Final = {discogs_id: _mbid(0xA0000 + index) for index, discogs_id in enumerate(ARTIST_IDS)}
RELEASE_MBIDS: Final = {discogs_id: _mbid(0xB0000 + index) for index, discogs_id in enumerate((*RELEASE_IDS, UNCATALOGUED_RELEASE_ID))}
LABEL_MBID: Final = _mbid(0xC0000)
MASTER_MBID: Final = _mbid(0xD0000)


def _relation(target_discogs_id: str, relation_type: str, direction: str | None = None) -> dict[str, Any]:
    """One relation in the shape both services read it out of an artist event.

    ``target_mbid`` is what this loader keys ``musicbrainz.relationships`` on and
    ``target_discogs_artist_id`` is what the enricher's ``MATCH`` binds, so a fixture
    relation carries both. A producer emits both for the same reason.
    """
    relation: dict[str, Any] = {
        "type": relation_type,
        "target_mbid": ARTIST_MBIDS[target_discogs_id],
        "target_type": "artist",
        "target_discogs_artist_id": int(target_discogs_id),
        "attributes": [],
    }
    if direction is not None:
        relation["direction"] = direction
    return relation


def _artist(discogs_id: str, name: str, relations: list[dict[str, Any]], links: list[dict[str, str]] | None = None) -> dict[str, Any]:
    mbid = ARTIST_MBIDS[discogs_id]
    return {
        "id": mbid,
        "mbid": mbid,
        "name": name,
        "discogs_artist_id": int(discogs_id),
        "relations": relations,
        "external_links": links or [],
    }


def _release(discogs_id: str, name: str, **media: Any) -> dict[str, Any]:
    mbid = RELEASE_MBIDS[discogs_id]
    return {"id": mbid, "mbid": mbid, "name": name, "status": "Official", "discogs_release_id": int(discogs_id), **media}


def _vinyl(entries: int) -> dict[str, Any]:
    """A legacy `media_raw` block of N identical 12" vinyl entries."""
    return {"media_raw": [{"format": '12" Vinyl', "position": position, "track_count": 9} for position in range(1, entries + 1)]}


# The canonical media block ADR 0007 publishes, hand-written so it can carry the
# empty-string entry a producer running a newer taxonomy can emit. Everything else
# in the fixture goes through `map_musicbrainz_release`, which never emits one.
_BLANK_MEDIUM_BLOCK: Final = {
    "media": {
        "taxonomy_version": "1",
        "items": [
            {"medium": "optical_cd", "family": "optical", "qty": 1, "position": 1},
            {"medium": "", "family": "", "qty": 1, "position": 2},
        ],
        "families": ["optical"],
        "unmapped": {"formats": [], "descriptions": []},
    }
}

_HOMEPAGE: Final = [{"url": "https://parity.invalid/artist", "service": "official homepage"}]
_DISCOGS_LINK: Final = [{"url": "https://parity.invalid/discogs", "service": "discogs"}]


def _events(*, first_round: bool) -> tuple[tuple[str, dict[str, Any]], ...]:
    """The fixture catalog, in the one order both services receive it.

    Round two is the second extraction of the same catalog. Four things change, and
    each is there to provoke a behaviour the two stores are predicted to disagree
    about:

    * artist 9001 no longer names its ``collaboration`` with 9003 — a relationship
      removed upstream between two extractions;
    * artist 9002 no longer carries its Discogs external link, which the child-row
      purge must remove even though nothing compares it;
    * release 9102 arrives with no media block at all; and
    * everything else is re-sent unchanged, so the purge has live rows to spare.
    """
    artist_relations = [
        _relation("9002", "member of band"),
        # Not in the enricher's map, so `graph.mb_relationship_type` returns NULL.
        _relation("9004", "producer"),
        # Reported from the wrong end: both services swap the endpoints, so the one
        # canonical edge 9004 -> 9001 is what each store ends up holding.
        _relation("9004", "supporting musician", direction="backward"),
    ]
    if first_round:
        artist_relations.insert(1, _relation("9003", "collaboration"))

    return (
        ("artists", _artist("9001", "Parity One", artist_relations, _HOMEPAGE)),
        ("artists", _artist("9002", "Parity Two", [_relation("9003", "artist rename")], _DISCOGS_LINK if first_round else [])),
        ("artists", _artist("9003", "Parity Three", [_relation("9004", "subgroup")])),
        (
            "artists",
            _artist(
                "9004",
                "Parity Four",
                [_relation("9001", "teacher"), _relation("9002", "tribute"), _relation("9003", "founder")],
            ),
        ),
        ("labels", {"id": LABEL_MBID, "mbid": LABEL_MBID, "name": "Parity Label", "discogs_label_id": int(LABEL_ID)}),
        (
            "release-groups",
            {"id": MASTER_MBID, "mbid": MASTER_MBID, "name": "Parity Group", "discogs_master_id": int(MASTER_ID)},
        ),
        # Two format entries resolving to one canonical medium: both sides must
        # collapse them into a single edge whose quantity is their sum.
        ("releases", _release("9101", "Double Vinyl", **_vinyl(2))),
        # Media in round one, none at all in round two.
        (
            "releases",
            _release("9102", "Compact Disc", **({"media_raw": [{"format": "CD", "position": 1, "track_count": 12}]} if first_round else {})),
        ),
        ("releases", _release("9103", "Blank Medium", **_BLANK_MEDIUM_BLOCK)),
        # A release naming a Discogs id no catalog holds. Its medium is one release
        # 9101 also names, so only the edge diverges and the vocabulary does not.
        ("releases", _release(UNCATALOGUED_RELEASE_ID, "Uncatalogued", **_vinyl(1))),
    )


def _signal(started_at: str) -> dict[str, Any]:
    """An ``extraction_complete`` body with every field the catalog-events v1 schema requires."""
    return {
        "type": "extraction_complete",
        "version": "2026-09-18",
        "timestamp": started_at,
        "started_at": started_at,
        "record_counts": dict.fromkeys(DATA_TYPES, 4),
    }


# ── The two stores, read back ────────────────────────────────────────────────


@dataclass(frozen=True)
class EdgeFamily:
    """One pair of statements reading the same edges out of the two stores.

    Both sides return ``(source, target, relationship_type, *properties)`` with the
    columns in one order, so the rows compare as plain positional tuples and the
    grouping into per-label sets is the same operation on both sides.
    """

    name: str
    statement: str
    cypher: str
    properties: tuple[str, ...]

    def describe(self) -> str:
        return f"{self.name} (PostgreSQL: {' '.join(self.statement.split())}\n      Neo4j: {' '.join(self.cypher.split())})"


# `graph.mb_rel_artist_artist` publishes `graph.mb_relationship_type(relationship_type)`
# as `relationship_type`, which is the mapping the acceptance asks for, applied by the
# schema rather than restated here. The view inner-joins both endpoint tables, so a
# relationship naming an mbid this loader has not stored is already dropped; the two
# joins below only fetch the Discogs ids the graph keys its vertices on.
#
# The `graph.member_of` arm is the Discogs half of the same label. It is unioned in
# because Neo4j cannot separate the two provenances — one `MEMBER_OF` edge carries one
# `source` property — and leaving it out would hide exactly the divergence this suite
# exists to record.
_ARTIST_RELATIONS_SQL = """
SELECT source_artist.discogs_artist_id::text AS source,
       target_artist.discogs_artist_id::text AS target,
       relationship.relationship_type        AS relationship_type,
       'musicbrainz'                         AS edge_source
FROM graph.mb_rel_artist_artist AS relationship
JOIN musicbrainz.artists AS source_artist ON source_artist.mbid = relationship.source_mbid
JOIN musicbrainz.artists AS target_artist ON target_artist.mbid = relationship.target_mbid
WHERE source_artist.discogs_artist_id IS NOT NULL
  AND target_artist.discogs_artist_id IS NOT NULL
UNION ALL
SELECT member_artist_id, group_artist_id, 'MEMBER_OF', 'discogs'
FROM graph.member_of
"""

_ARTIST_RELATIONS_CYPHER = """
MATCH (source:Artist)-[edge]->(target:Artist)
WHERE type(edge) IN $labels
RETURN source.id AS c0, target.id AS c1, type(edge) AS c2, edge.source AS c3
"""

EDGE_FAMILIES: Final[tuple[EdgeFamily, ...]] = (
    EdgeFamily("artist relations", _ARTIST_RELATIONS_SQL, _ARTIST_RELATIONS_CYPHER, ("source",)),
    EdgeFamily(
        "release media",
        "SELECT release_id, medium_id, 'ISSUED_ON', source, qty FROM graph.issued_on",
        "MATCH (release:Release)-[edge:ISSUED_ON]->(medium:Medium) "
        "RETURN release.id AS c0, medium.id AS c1, 'ISSUED_ON' AS c2, edge.source AS c3, edge.qty AS c4",
        ("source", "qty"),
    ),
    EdgeFamily(
        "media vocabulary",
        "SELECT medium_id, family_name, 'IN_FAMILY' FROM graph.in_family",
        "MATCH (medium:Medium)-[:IN_FAMILY]->(family:MediaFamily) RETURN medium.id AS c0, family.name AS c1, 'IN_FAMILY' AS c2",
        (),
    ),
)

Tuples = frozenset[tuple[Any, ...]]


@dataclass(frozen=True)
class Divergence:
    """What one edge label holds in one store and not the other."""

    only_in_postgres: Tuples = frozenset()
    only_in_neo4j: Tuples = frozenset()

    def __bool__(self) -> bool:
        return bool(self.only_in_postgres or self.only_in_neo4j)


@dataclass(frozen=True)
class ParityRun:
    """Both stores after the fixture events, indexed by edge label."""

    postgres: dict[str, Tuples] = field(default_factory=dict)
    neo4j: dict[str, Tuples] = field(default_factory=dict)
    divergences: dict[str, Divergence] = field(default_factory=dict)
    family_of: dict[str, EdgeFamily] = field(default_factory=dict)
    deleted: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class Cause:
    """One reason an edge label diverges, with the tuples that reason accounts for."""

    reason: str
    only_in_postgres: Tuples = frozenset()
    only_in_neo4j: Tuples = frozenset()


@dataclass(frozen=True)
class ExpectedDifference:
    """Every reason one edge label is predicted to diverge, and by exactly how much."""

    causes: tuple[Cause, ...]

    @property
    def only_in_postgres(self) -> Tuples:
        return frozenset().union(*(cause.only_in_postgres for cause in self.causes))

    @property
    def only_in_neo4j(self) -> Tuples:
        return frozenset().union(*(cause.only_in_neo4j for cause in self.causes))

    @property
    def reasons(self) -> str:
        return "".join(f"\n      - {cause.reason}" for cause in self.causes)


EXPECTED_DIFFERENCES: Final[dict[str, ExpectedDifference]] = {
    "MEMBER_OF": ExpectedDifference(
        (
            Cause(
                reason=(
                    "Post-merge source stamping. The enricher writes an artist relation as "
                    "`MERGE (a)-[r:MEMBER_OF]->(b) SET r.source = 'musicbrainz'`, so `source` is not part "
                    "of the merge key and a Discogs-derived edge that already joins the same pair is "
                    "adopted and retro-stamped. Neo4j can then hold only one provenance for the pair, and "
                    "the Discogs one is lost. PostgreSQL keeps the two apart by construction: the Discogs "
                    "membership is a `graph.member_of` row and the MusicBrainz one is a "
                    "`musicbrainz.relationships` row, so both survive. Measured as a gap in the coverage "
                    "spike gm-database-schema-9c8.2; it is the enricher's behaviour that is lossy, and "
                    "retiring the enricher is what resolves it."
                ),
                only_in_postgres=frozenset({("9001", "9002", "MEMBER_OF", "discogs")}),
            ),
        )
    ),
    "COLLABORATED_WITH": ExpectedDifference(
        (
            Cause(
                reason=(
                    "The enricher never deletes an artist relationship edge. Its only DELETE is the "
                    "source-scoped `ISSUED_ON` stale sweep, so a relation dropped between two extractions "
                    "survives in Neo4j forever. This loader's delete-reconciliation "
                    "(gm-musicbrainz-sql-loader-0fc.2) removes the row whose `updated_at` predates the "
                    "extraction's `started_at`, which is why the collaboration 9001 -> 9003 is in Neo4j "
                    "and not in PostgreSQL. The divergence is the enricher's gap, and the loader is the "
                    "side that is right."
                ),
                only_in_neo4j=frozenset({("9001", "9003", "COLLABORATED_WITH", "musicbrainz")}),
            ),
        )
    ),
    UNMAPPED: ExpectedDifference(
        (
            Cause(
                reason=(
                    "A relation type the enricher's MB_RELATIONSHIP_MAP does not hold. The enricher "
                    "`continue`s and writes no edge at all; this loader stores the raw string, and "
                    "`graph.mb_relationship_type` returns NULL for it. The relational store is therefore "
                    "strictly richer — the relation is still queryable through "
                    "`raw_relationship_type` — and nothing is lost by the difference. It is declared "
                    "rather than filtered out so that widening the map, on either side, shows up here."
                ),
                only_in_postgres=frozenset({("9001", "9004", None, "musicbrainz")}),
            ),
        )
    ),
    "ISSUED_ON": ExpectedDifference(
        (
            Cause(
                reason=(
                    "A MusicBrainz release naming a Discogs id the catalog does not hold. This loader "
                    "writes `graph.issued_on` under that id because no edge relation in the `graph` schema "
                    "declares a foreign key, by design: a loader writes an edge in the same transaction as "
                    "the document it came from and may name an entity it has not ingested yet. The "
                    "enricher's `MATCH (r:Release {id: $discogs_id})` binds nothing, so it counts the "
                    "record under `entities_skipped_no_discogs_match` and reconciles no media. Raised in "
                    "the gm-musicbrainz-sql-loader-0fc.3 review; the row is a correct projection of the "
                    "stored media block that neither the schema's own projection nor Neo4j can show."
                ),
                only_in_postgres=frozenset({(UNCATALOGUED_RELEASE_ID, "vinyl_12", "ISSUED_ON", "musicbrainz", 1)}),
            ),
            Cause(
                reason=(
                    "An event carrying no media block at all. The enricher's `release_media_block` returns "
                    "None and `reconcile_release_media` returns early, reading the absence as 'unknown' and "
                    "leaving the edges it wrote last time in place. This loader's `release_media_block` "
                    "falls through to the mapper, which yields an empty `items` list, and the "
                    "delete-then-insert prunes the release's rows for this source. The two readings of an "
                    "absent block are genuinely different, and the loader's is the one the schema's own "
                    "projection of the stored block agrees with."
                ),
                only_in_neo4j=frozenset({("9102", "optical_cd", "ISSUED_ON", "musicbrainz", 1)}),
            ),
            Cause(
                reason=(
                    "An empty-string medium in a canonical media block. The enricher's `media_edge_rows` "
                    'checks `isinstance(..., str)` only, so `""` passes and it merges `Medium {id: ""}` '
                    "and an edge to it. This loader requires a non-empty medium and family, which is also "
                    "what the schema's `_MEDIA_SOURCE` requires through `_non_empty`, so the entry "
                    "contributes nothing. The loader and the schema projection agree; the enricher writes "
                    "a vertex with no identity."
                ),
                only_in_neo4j=frozenset({("9103", "", "ISSUED_ON", "musicbrainz", 1)}),
            ),
        )
    ),
    "IN_FAMILY": ExpectedDifference(
        (
            Cause(
                reason=(
                    "The same empty-string medium, one relation further out. The enricher merges "
                    '`MediaFamily {name: ""}` and an `IN_FAMILY` edge into it; this loader never adds the '
                    "medium or the family to the shared vocabulary, so `graph.in_family`, which is a "
                    "projection of `graph.medium` joined to `graph.media_family`, has nothing to publish."
                ),
                only_in_neo4j=frozenset({("", "", "IN_FAMILY")}),
            ),
        )
    ),
}


# ── Driving both services ────────────────────────────────────────────────────


class Delivery:
    """A broker delivery reduced to what both handlers touch, settled at most once."""

    def __init__(self, body: bytes) -> None:
        self.body = body
        self.headers: dict[str, Any] = {}
        self.settlements: list[tuple[str, bool | None]] = []

    def _record(self, operation: str, requeue: bool | None) -> None:
        if self.settlements:
            raise AssertionError("delivery was settled more than once")
        self.settlements.append((operation, requeue))

    async def ack(self) -> None:
        self._record("ack", None)

    async def nack(self, *, requeue: bool) -> None:
        self._record("nack", requeue)


_ENRICHER_HANDLERS = {
    "artists": "on_artist_message",
    "labels": "on_label_message",
    "release-groups": "on_release_group_message",
    "releases": "on_release_message",
}


async def _deliver(data_type: str, payload: dict[str, Any], pool: AsyncPostgreSQLPool, driver: Any, purge: StaleChildRowPurge) -> None:
    """Hand one event to both services' real delivery handlers, loader first."""
    body = dumps(payload)

    with (
        patch.object(loader, "shutdown_requested", False),
        patch.object(loader, "connection_pool", pool),
        patch.object(loader, "stale_row_purge", purge),
    ):
        loaded = await loader.on_data_message(Delivery(body), data_type)
    assert loaded.settlement is Settlement.ACK, f"the loader refused a fixture {data_type} event: {loaded}"

    with patch.object(enricher, "shutdown_requested", False), patch.object(enricher, "graph", driver):
        enriched = await getattr(enricher, _ENRICHER_HANDLERS[data_type])(Delivery(body))
    assert enriched.settlement is Settlement.ACK, f"the enricher refused a fixture {data_type} event: {enriched}"


# One statement per vertex kind rather than one chained query: a seed is not the
# thing under test, and a `WITH`-threaded chain would be the most fragile code here.
_SEED_STATEMENTS: Final = (
    ("UNWIND $ids AS id MERGE (:Artist {id: id})", "artists"),
    ("UNWIND $ids AS id MERGE (:Release {id: id})", "releases"),
    ("UNWIND $ids AS id MERGE (:Label {id: id})", "labels"),
    ("UNWIND $ids AS id MERGE (:Master {id: id})", "masters"),
)

_SEED_IDS: Final = {
    "artists": list(ARTIST_IDS),
    "releases": list(RELEASE_IDS),
    "labels": [LABEL_ID],
    "masters": [MASTER_ID],
}

# The membership `discogs-graph-enricher` already wrote, carrying its own provenance.
# The MusicBrainz fixture declares the same membership, so the enricher's post-merge
# `SET r.source` lands on this edge — which is the divergence the registry records.
_SEED_MEMBERSHIP: Final = (
    "MATCH (member:Artist {id: $member}), (grouping:Artist {id: $grouping}) MERGE (member)-[:MEMBER_OF {source: 'discogs'}]->(grouping)"
)


async def _seed(pool: AsyncPostgreSQLPool, driver: Any) -> None:
    """Put the Discogs half of the catalog into both stores, identically.

    Neither service creates an entity vertex — the enricher only ever `MATCH`es one,
    and this loader writes `graph.issued_on` against a key the Discogs loader owns —
    so a parity run that seeded only one store would be measuring the seed.
    """
    async with driver.session(database="neo4j") as session:
        for statement, kind in _SEED_STATEMENTS:
            result = await session.run(statement, ids=_SEED_IDS[kind])
            await result.consume()
        result = await session.run(_SEED_MEMBERSHIP, member=ARTIST_IDS[0], grouping=ARTIST_IDS[1])
        await result.consume()

    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        await conn.execute(
            "INSERT INTO graph.member_of (member_artist_id, group_artist_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (ARTIST_IDS[0], ARTIST_IDS[1]),
        )


async def _age_every_child_row(pool: AsyncPostgreSQLPool) -> str:
    """Backdate both child tables and return the boundary the next extraction starts at.

    Standing in for a first extraction that ran earlier rather than milliseconds ago.
    Only `updated_at` moves, and nothing compares it: it exists so the purge's
    `updated_at < started_at` test is decided by the fixture rather than by how fast
    this test happens to run.
    """
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        for table in (RELATIONSHIPS, EXTERNAL_LINKS):
            await conn.execute(f"UPDATE {table} SET updated_at = NOW() - INTERVAL '1 day'")  # noqa: S608
        cursor = await conn.execute("SELECT NOW()")
        row = await cursor.fetchone()
    assert row is not None
    boundary: str = row[0].isoformat()
    return boundary


class _RecordingPurge(StaleChildRowPurge):
    """The production purge, with the counts the service discards kept for assertion.

    `_reconcile_deleted_child_rows` throws the return value away — nothing in the
    service needs it — but this suite's COLLABORATED_WITH difference is only evidence
    if the purge really fired, so the last non-empty result is kept here rather than
    inferred from the tables afterwards.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.deleted: dict[str, int] = {}

    async def purge(self) -> dict[str, int]:
        deleted = await super().purge()
        if deleted:
            self.deleted = deleted
        return deleted


async def _load(pool: AsyncPostgreSQLPool, driver: Any) -> dict[str, int]:
    """Play both extractions into both services and return the purge's delete counts.

    The loader's startup sequence runs first, in full. `main` probes for the
    delete-reconciliation column and only then arms the writer's refresh clause and
    constructs the purge; doing the same here is what makes the second extraction's
    reconciliation real rather than a flag this test set for itself. Against an
    unpromoted schema the probe would answer False and the assertion below would say
    so, rather than the run quietly comparing against a loader with the purge off.
    """
    enabled = await reconciliation_columns_present(pool, logger)
    assert enabled is True, "the parity lane needs the promoted schema: the purge is part of what it compares"
    loader._persistence_writer.set_refresh_updated_at(enabled)

    purge = _RecordingPurge(pool, logger)
    await _seed(pool, driver)

    for data_type, payload in _events(first_round=True):
        await _deliver(data_type, payload, pool, driver, purge)

    boundary = await _age_every_child_row(pool)

    for data_type, payload in _events(first_round=False):
        await _deliver(data_type, payload, pool, driver, purge)

    # The four `extraction_complete` signals close the second extraction. Only the
    # last one can fire the purge: both child tables are written by all four entity
    # kinds, so purging earlier would delete what the others have not sent yet.
    signal = _signal(boundary)
    for index, data_type in enumerate(DATA_TYPES):
        await _deliver(data_type, signal, pool, driver, purge)
        assert purge.is_latched() is (index == len(DATA_TYPES) - 1)

    return dict(purge.deleted)


async def _read_postgres(pool: AsyncPostgreSQLPool, statement: str) -> list[tuple[Any, ...]]:
    async with pool.connection() as conn:
        cursor = await conn.execute(statement)
        return [tuple(row) for row in await cursor.fetchall()]


async def _read_neo4j(driver: Any, cypher: str, **parameters: Any) -> list[tuple[Any, ...]]:
    async with driver.session(database="neo4j") as session:
        result = await session.run(cypher, **parameters)
        return [tuple(record.values()) async for record in result]


def _by_label(rows: list[tuple[Any, ...]]) -> dict[str, Tuples]:
    """Group rows by the edge label in their third slot, unmapped ones under the sentinel."""
    grouped: dict[str, set[tuple[Any, ...]]] = {}
    for row in rows:
        grouped.setdefault(UNMAPPED if row[2] is None else str(row[2]), set()).add(row)
    return {label: frozenset(tuples) for label, tuples in grouped.items()}


async def _compare(pool: AsyncPostgreSQLPool, driver: Any) -> ParityRun:
    """Read every mapped edge out of both stores and diff them per label."""
    run = ParityRun()
    labels = sorted(set(enricher.MB_RELATIONSHIP_MAP.values()))
    for family in EDGE_FAMILIES:
        postgres = _by_label(await _read_postgres(pool, family.statement))
        parameters = {"labels": labels} if "$labels" in family.cypher else {}
        neo4j = _by_label(await _read_neo4j(driver, family.cypher, **parameters))
        for label in sorted(postgres.keys() | neo4j.keys()):
            left = postgres.get(label, frozenset())
            right = neo4j.get(label, frozenset())
            run.postgres[label] = left
            run.neo4j[label] = right
            run.divergences[label] = Divergence(left - right, right - left)
            run.family_of[label] = family
    return run


def _describe(run: ParityRun, label: str) -> str:
    divergence = run.divergences[label]
    return (
        f"\n  {label}, read by {run.family_of[label].describe()}"
        f"\n    only in PostgreSQL: {sorted(divergence.only_in_postgres, key=repr)}"
        f"\n    only in Neo4j:      {sorted(divergence.only_in_neo4j, key=repr)}"
    )


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def postgres_pool() -> AsyncIterator[AsyncPostgreSQLPool]:
    """A pool on the promoted schema, emptied of every relation this suite reads."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("the parity lane needs TEST_DATABASE_URL; run `just test-parity`")

    connection_params: dict[str, Any] = conninfo_to_dict(database_url)
    pool = AsyncPostgreSQLPool(connection_params=connection_params, max_retries=1)
    await pool.initialize()
    failures = await create_postgres_schema(pool)
    assert failures == 0, f"{failures} schema statements failed while applying the promoted schema"
    try:
        await _empty_postgres(pool)
        yield pool
    finally:
        await _empty_postgres(pool)
        await pool.close()


_TRUNCATED = (
    "musicbrainz.relationships",
    "musicbrainz.external_links",
    "musicbrainz.artists",
    "musicbrainz.labels",
    "musicbrainz.releases",
    "musicbrainz.release_groups",
    "graph.issued_on",
    "graph.medium",
    "graph.media_family",
    "graph.member_of",
)


async def _empty_postgres(pool: AsyncPostgreSQLPool) -> None:
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        # Every name here is one of this module's own constants, never input.
        await conn.execute(f"TRUNCATE {', '.join(_TRUNCATED)}")


@pytest_asyncio.fixture
async def neo4j_driver() -> AsyncIterator[Any]:
    """A driver on the disposable Neo4j, emptied before and after the run."""
    uri = os.environ.get("NEO4J_URI")
    password = os.environ.get("NEO4J_INTEGRATION_PASSWORD")
    if not uri or not password:
        pytest.skip("the parity lane needs NEO4J_URI and NEO4J_INTEGRATION_PASSWORD; run `just test-parity`")

    from neo4j import AsyncGraphDatabase

    driver = AsyncGraphDatabase.driver(uri, auth=(os.environ.get("NEO4J_INTEGRATION_USER", "neo4j"), password))
    await driver.verify_connectivity()
    try:
        await _empty_neo4j(driver)
        yield driver
    finally:
        with suppress(Exception):
            await _empty_neo4j(driver)
        await driver.close()


async def _empty_neo4j(driver: Any) -> None:
    async with driver.session(database="neo4j") as session:
        result = await session.run("MATCH (node) DETACH DELETE node")
        await result.consume()


@pytest_asyncio.fixture
async def parity(postgres_pool: AsyncPostgreSQLPool, neo4j_driver: Any) -> AsyncIterator[ParityRun]:
    """Play the fixture events through both services and read both stores back."""
    try:
        deleted = await _load(postgres_pool, neo4j_driver)
        run = await _compare(postgres_pool, neo4j_driver)
        run.deleted.update(deleted)
        yield run
    finally:
        # `_load` arms the module-level writer the way `main` does; put it back so a
        # later test in the same process sees the service as it is before startup.
        loader._persistence_writer.set_refresh_updated_at(False)


# ── The claims ───────────────────────────────────────────────────────────────


def test_the_pinned_enricher_is_the_one_under_comparison() -> None:
    """A parity run states exactly which enricher it proved the loader equal to."""
    expected = os.environ.get("PARITY_ENRICHER_REVISION")
    if not expected:
        pytest.skip("PARITY_ENRICHER_REVISION is set by `just test-parity`")

    direct_url = importlib.metadata.distribution(ENRICHER_DISTRIBUTION).read_text("direct_url.json")
    assert direct_url is not None, f"{ENRICHER_DISTRIBUTION} was not installed from a pinned revision"
    installed = json.loads(direct_url)["vcs_info"]["commit_id"]
    assert installed == expected, f"the parity lane pinned {expected} but {ENRICHER_DISTRIBUTION} {installed} is installed"


def test_the_registry_names_only_labels_the_vocabulary_can_produce() -> None:
    """A registry key that no edge family can ever emit is a stale entry, not a difference."""
    producible = set(enricher.MB_RELATIONSHIP_MAP.values()) | {UNMAPPED, "ISSUED_ON", "IN_FAMILY"}
    unreachable = set(EXPECTED_DIFFERENCES) - producible
    assert not unreachable, f"registry keys outside the compared vocabulary: {sorted(unreachable)}"


@pytest.mark.asyncio
async def test_both_stores_hold_the_catalog_the_fixture_states(parity: ParityRun) -> None:
    """A comparison of two empty stores must not be able to pass."""
    expected = {"MEMBER_OF", "SUPPORTED", "RENAMED_TO", "SUBGROUP_OF", "TAUGHT", "TRIBUTE_TO", "FOUNDED", "ISSUED_ON", "IN_FAMILY"}
    empty_in_postgres = sorted(label for label in expected if not parity.postgres.get(label))
    empty_in_neo4j = sorted(label for label in expected if not parity.neo4j.get(label))
    assert empty_in_postgres == [], f"the loader wrote nothing for {empty_in_postgres}"
    assert empty_in_neo4j == [], f"the enricher wrote nothing for {empty_in_neo4j}"


@pytest.mark.asyncio
async def test_the_second_extraction_reconciled_the_child_rows(parity: ParityRun) -> None:
    """The COLLABORATED_WITH difference is only meaningful if the purge actually ran."""
    assert parity.deleted == {RELATIONSHIPS: 1, EXTERNAL_LINKS: 1}, (
        f"the parity run depends on the delete-reconciliation firing on the fourth extraction_complete; it reported {parity.deleted}"
    )


@pytest.mark.asyncio
async def test_every_label_outside_the_registry_is_identical_in_both_stores(parity: ParityRun) -> None:
    """The acceptance: equality per edge label, except where a reason is on record."""
    undeclared = sorted(label for label, divergence in parity.divergences.items() if divergence and label not in EXPECTED_DIFFERENCES)
    assert not undeclared, "the two stores disagree about edge labels no expected difference covers:" + "".join(
        _describe(parity, label) for label in undeclared
    )


@pytest.mark.asyncio
async def test_every_declared_difference_materialises_exactly_as_declared(parity: ParityRun) -> None:
    """A registry entry that stopped being true must fail, not quietly stay on the books."""
    stale = sorted(label for label in EXPECTED_DIFFERENCES if not parity.divergences.get(label))
    assert stale == [], f"declared differences that did not materialise on this fixture: {stale}"

    for label, expected in sorted(EXPECTED_DIFFERENCES.items()):
        divergence = parity.divergences[label]
        assert divergence.only_in_postgres == expected.only_in_postgres, (
            f"{label}: the PostgreSQL-only tuples moved{_describe(parity, label)}\n    reasons on record:{expected.reasons}"
        )
        assert divergence.only_in_neo4j == expected.only_in_neo4j, (
            f"{label}: the Neo4j-only tuples moved{_describe(parity, label)}\n    reasons on record:{expected.reasons}"
        )


@pytest.mark.asyncio
async def test_the_differences_predicted_to_be_resolved_are_the_ones_that_are(parity: ParityRun) -> None:
    """The three shapes the coverage spike flagged, each now agreeing in both stores."""
    # The vocabulary difference, resolved by `graph.mb_relationship_type`: the loader
    # stores `member of band` and Neo4j holds a `MEMBER_OF` type, and the mapping is
    # what makes those the same edge.
    assert ("9001", "9002", "MEMBER_OF", "musicbrainz") in parity.postgres["MEMBER_OF"]
    assert ("9001", "9002", "MEMBER_OF", "musicbrainz") in parity.neo4j["MEMBER_OF"]

    # `direction: backward`: both services swap the endpoints before writing, so the
    # canonical edge runs 9004 -> 9001 in both stores and there is only one of it.
    assert parity.postgres["SUPPORTED"] == parity.neo4j["SUPPORTED"] == frozenset({("9004", "9001", "SUPPORTED", "musicbrainz")})

    # Two format entries resolving to one canonical medium collapse to one edge whose
    # quantity is their sum, identically on both sides.
    assert ("9101", "vinyl_12", "ISSUED_ON", "musicbrainz", 2) in parity.postgres["ISSUED_ON"]
    assert ("9101", "vinyl_12", "ISSUED_ON", "musicbrainz", 2) in parity.neo4j["ISSUED_ON"]
