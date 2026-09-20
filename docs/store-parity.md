# Cross-store parity with the graph enricher

Phase 4 retires [`musicbrainz-graph-enricher`](https://github.com/groovemap-music/musicbrainz-graph-enricher)
only if the relational edges this loader writes are the edges the enricher writes. That
is a claim about two running systems, so `just test-parity` demonstrates it rather than
assuming it: the same fixture events go into both services, and the two graphs are read
back and compared per edge label.

The lane is opt-in and outside `just check`, because it is the only one in this
repository that needs a second engine. It is not in CI either, for the same reason
`discogs-sql-loader`'s equivalent lane is not.

```bash
just test-parity
```

## How both sides are driven

By their own delivery handlers, not by a reimplementation of either.

| Side | Entry point | Store |
| --- | --- | --- |
| This loader | `brainztableinator.on_data_message` | `postgres:18-alpine`, promoted schema applied by `groovemap-database-schema` |
| The enricher | `brainzgraphinator.on_artist_message` and its three siblings | `neo4j:2026-community` |

Each handler is patched onto a live connection exactly as that service's own integration
suite patches it, so the comparison measures the code that ships. The loader's startup
sequence runs in full first: the delete-reconciliation probe answers against the promoted
schema, and only then is the writer's refresh clause armed and the purge constructed. The
fixture's second extraction therefore reconciles for real, which is what makes one of the
differences below evidence rather than an artefact.

The enricher is installed by `scripts/test-parity.sh` with
`uv pip install --no-deps`, pinned to a revision the run reports, and is deliberately
**not** declared in `pyproject.toml`. It pins its own `groovemap-runtime` revision, which
is a hard URL conflict with the one this repository's persistence contract pins, and
declaring it would reach every other lane including `install-check`. `--no-deps` keeps
this repository's runtime in place, which is the answer parity wants anyway: both stores
must derive media from one `common.media`, or the comparison would be measuring a runtime
skew rather than the loader. `uv sync` removes the package again, and
`test_the_pinned_enricher_is_the_one_under_comparison` fails if the installed revision is
not the pinned one.

Both stores are seeded with the Discogs half of the catalog before any event is
delivered — the `:Artist`, `:Release`, `:Label` and `:Master` nodes and the one
`graph.member_of` row. Neither service creates an entity vertex: the enricher only ever
`MATCH`es one, and this loader writes `graph.issued_on` under a key `discogs-sql-loader`
owns. A run that seeded one store would be measuring the seed.

## What is compared

Three families of edge are read out of both stores and then grouped by edge label:

- the MusicBrainz artist-to-artist relations, whose label comes from
  `graph.mb_relationship_type` — the schema function rendering the enricher's own
  `MB_RELATIONSHIP_MAP` — unioned with the Discogs-provenance `MEMBER_OF` rows they share
  a label with;
- `ISSUED_ON`, the shared release-to-medium edge both loaders write, with its `source` and
  `qty` properties; and
- `IN_FAMILY`, the medium-to-family edge derived from the media vocabulary.

A tuple is `(source, target, mapped relationship_type, *properties)` on both sides, so
equality is set equality over identical shapes. Node properties are out of scope: the
enricher stamps `mb_updated_at` with `datetime.now()`, which would make every comparison
non-deterministic.

## The fixture

Two rounds of the same four-entity-kind catalog, so pruning and reconciliation are
observable, followed by the four `extraction_complete` signals that close the second
extraction. It is built to provoke each shape the comparison has to decide:

- a relationship **removed between the two rounds** (artist 9001 stops naming its
  collaboration with 9003);
- a relation reported with **`direction: backward`**, which both services swap before
  writing;
- a **relation type the enricher's map does not hold** (`producer`);
- a **Discogs-provenance `MEMBER_OF`** already in both stores, which the MusicBrainz
  event also declares;
- a release whose **several media entries collapse to one medium** (2×12″ vinyl);
- a release that **loses its media block entirely** in round two;
- a canonical media block carrying an **empty-string medium and family**; and
- a release **naming a Discogs id the catalog does not hold**.

## The expected-differences registry

`EXPECTED_DIFFERENCES` is a plain mapping keyed by edge label, with one recorded cause per
reason. It is held in **both** directions: an undeclared divergence fails and names the
statements it came from, and a declared difference that stops materialising fails too.

| Edge label | Shape of the difference | Cause |
| --- | --- | --- |
| `MEMBER_OF` | one tuple only in PostgreSQL | The enricher's post-merge source stamping. `MERGE (a)-[r:MEMBER_OF]->(b) SET r.source = 'musicbrainz'` leaves `source` out of the merge key, so a Discogs-derived edge joining the same pair is adopted and retro-stamped. Neo4j can hold only one provenance per pair; PostgreSQL keeps `graph.member_of` and `musicbrainz.relationships` apart by construction. Measured in the coverage spike `gm-database-schema-9c8.2`. |
| `COLLABORATED_WITH` | one tuple only in Neo4j | The enricher never deletes an artist relationship edge — its only `DELETE` is the source-scoped `ISSUED_ON` sweep — so a relation dropped between extractions survives forever. This loader's delete-reconciliation removes it. The loader is the side that is right. |
| `<unmapped>` | one tuple only in PostgreSQL | A relation type outside `MB_RELATIONSHIP_MAP`. The enricher writes no edge; this loader stores the raw string and `graph.mb_relationship_type` returns `NULL`. The relational store is strictly richer, the relation still being queryable through `raw_relationship_type`. |
| `ISSUED_ON` | one tuple only in PostgreSQL, two only in Neo4j | Three causes: a release naming a Discogs id no catalog holds (no `graph` edge relation declares a foreign key, so the loader writes the row; the enricher's `MATCH` binds nothing); an event with no media block at all (the enricher reads the absence as "unknown" and returns early, the loader reads it as "empty" and prunes); and an empty-string medium (the enricher's `isinstance(..., str)` check admits it, the loader and the schema's own projection require a non-empty value). |
| `IN_FAMILY` | one tuple only in Neo4j | The same empty-string medium, one relation further out: the enricher merges `MediaFamily {name: ""}` and an edge into it, while `graph.in_family` has no medium or family row to publish. |

Every other compared label is identical in both stores. Three shapes the coverage spike
flagged as candidates are asserted to have been **resolved** rather than recorded here:
the vocabulary difference, which `graph.mb_relationship_type` closes; `direction:
backward`, which both services canonicalise the same way; and the media collapse, which
both sides sum into one edge.
