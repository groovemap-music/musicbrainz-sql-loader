# Write-path throughput audit, 2026-11-04

Bead `gm-musicbrainz-sql-loader-5wwo`. Read-only audit of how fast this loader can commit a
full MusicBrainz extraction to PostgreSQL, done before MusicBrainz is enabled on lux. No
production code changed.

## Scope and honesty about the evidence

- **Static review is the primary result.** Every statement the loader issues was read in
  `_persistence.py`, `_record_processing.py`, the delivery path in `brainztableinator.py`,
  the shared alias helpers in `python-libraries` (`common/identity.py`) and the DDL and
  indexes in `database-schema` (`postgres.py`, pinned 0.6.1).
- **The timings are from synthetic records, not a real MusicBrainz sample.** No real sample
  was available without touching lux, which this audit must not do. Records were generated
  with MusicBrainz-like shapes: per artist 25 relations and 6 links; per release 12
  relations and 3 links, one media item, and a Discogs id on one third of releases; per
  release group 3 relations and 2 links; per label 5 relations and 3 links. The mix was 60%
  releases, 25% artists, 10% release groups, 5% labels. **The per-record relation and link
  counts are assumptions.** Relationship volume is the main driver of cost, so every
  absolute number below moves with it.
- **Partial production data makes any lux timing a lower bound.** Postgres on lux holds
  3.97M of 19.6M releases and 7.9M of 10.3M artists; labels and release groups are
  complete. Btree depth, cache hit rate and checkpoint pressure all degrade as the tables
  fill, so rates measured on small tables are optimistic. Each scaling note says how.
- Setup: `postgres:18-alpine` in Docker on macOS, loopback, `fsync=on`, default settings,
  schema from `create_postgres_schema` (database-schema 0.6.1), 2,000 records per run,
  `pg_stat_statements` for per-statement numbers. Harness lived outside the repo; databases,
  containers and anonymous volumes were removed afterwards.
- The Discogs loader has no dated audit to copy a method from. Its `docs/performance-guide.md`
  describes the write path and says to measure batch size, flush duration and outcomes over a
  representative extraction. This audit follows that: per-statement breakdown, one variable
  changed at a time, then a scaling estimate.

## How the loader writes today

| Property | musicbrainz-sql-loader | discogs-sql-loader (for comparison) |
| --- | --- | --- |
| Unit of work | **One transaction per delivery** (`_handle_data_message`) | Per-entity in-memory batch, bulk upsert per flush |
| Commit | One commit per record | One commit per `POSTGRES_BATCH_SIZE` (default 100) |
| Concurrency | `WriteAdmission(pool_max)` = 12 slots; prefetch 12 per consumer | At most 2 concurrent flushes |
| Child rows | `executemany`, one statement per relationship or link | Bulk |

This loader has no batch mode at all. That is the largest structural difference from the
sibling and the reason the Discogs comparison matters.

## Per-statement breakdown (one record, current code)

Counts are per record averaged over the 2,000-record run; server time is `pg_stat_statements`
mean execution time per call on near-empty tables (a lower bound). "Round trips" counts
client-visible waits, treating each `executemany` as one pipelined wait.

| Statement | Fired for | Calls per record | Mean server ms/call | Notes |
| --- | --- | --- | --- | --- |
| `resolve_aliases` SELECT (`unnest` join on `provider_aliases`) | every record | 1 to 2 | 0.44 cold, 0.02 warm | One more for the Discogs ref when a Discogs id is present |
| `INSERT catalog_items` + `INSERT provider_aliases` (mint) | new MBID | 2 | 0.04 + 0.07 | Skipped on rerun |
| `attach_aliases` INSERT (+ SELECT on conflict) | record with a Discogs id; each release with barcode or catalog number | 0 to 2 | 0.07 | Release identifier aliases run a second attach |
| Entity upsert (CTE: upsert, `old`, `identity_link_events`) | every record | 1 | 0.10 to 0.36 | `old` re-reads the row by PK; the event insert almost never fires |
| `INSERT musicbrainz.relationships ... ON CONFLICT` | every relation | ~14 rows | 0.04 to 0.06 | Largest total server time (about 1.1 s of about 3 s) |
| `INSERT musicbrainz.external_links ... ON CONFLICT` | every link | ~3.6 rows | 0.03 to 0.05 | |
| `graph.media_family`, `graph.medium` upserts | release with a Discogs id | 2 | 0.02 | |
| `DELETE graph.issued_on` then `INSERT` | release with a Discogs id | 2 | 0.02 | |
| `COMMIT` | every record | 1 | n/a | 2,000 commits for 2,000 records |

Per release: about 6 to 9 sequential round trips, plus the commit. Server execution accounts
for only about 3 s of the 20 s wall time for 2,000 records. The rest is client round-trip
and Python time, which is why the experiments below behave as they do.

## Experiments (2,000 records each, fresh database unless noted)

| Run | Wall | Rate | Commits | Reading |
| --- | --- | --- | --- | --- |
| A. Current: one transaction per record, sequential | 20.0 s | 100 rec/s | ~4,300 (includes schema setup) | Baseline |
| B. Same code, 100 records per transaction | 20.0 s | 100 rec/s | ~460 | **No change**: commit and fsync are not the bottleneck here |
| C. Same code as production, 12 concurrent connections | 8.1 s | 247 rec/s | ~3,900 | 2.5x: the loop is latency-bound, so concurrency helps |
| D. Rerun of A over identical records (all conflicts) | 20.8 s | 96 rec/s | n/a | Same speed, but see bloat below |

Interpretation:

- **Fewer commits alone do not help** (B equals A). Do not file "batch the commits" as the
  fix. The Discogs lesson does not transfer as a commit-count lever; what would transfer is
  fewer round trips per record (see ranked list).
- The service already runs C-style concurrency in production (12 slots per pool), so 247 rec/s
  on this laptop is the closest figure to the deployed shape. lux has real network latency
  to Postgres and a durable-storage fsync, both of which this loopback setup understates.

### Rerun and update cost (run D, relationships)

| | After first load | After rerun |
| --- | --- | --- |
| Heap | 3.9 MB | 4.8 MB |
| Indexes | 9.8 MB | 13 MB |
| Updates / HOT updates | 0 / 0 | 26,948 / **0** |
| Dead tuples | 0 | 9,271 |

Every conflict rewrites the row, because `DO UPDATE SET ended = EXCLUDED.ended` plus
`updated_at = NOW()` always produces a new tuple. HOT updates are zero because `updated_at` is
indexed (`idx_mb_rels_updated_at`), so each rerun also adds an entry to every one of the 8
indexes on `relationships`. A re-extraction therefore costs as much as a first load and grows
the table and indexes by about a quarter per pass until autovacuum catches up.

### Size per relationship

`relationships` carried about 144 B heap and about 360 B of index per row across 8 indexes
(primary key, `relationships_natural_key`, source, target, type, two endpoint pairs,
`updated_at`). The index bytes are 2.5x the heap. `relationships_natural_key` spans eight
columns including a `jsonb`, and its keys are random UUIDs, so inserts land in random index
pages: cheap while the index fits in `shared_buffers`, expensive once it does not.

## Ranked improvement list

Ordered by expected effect on full-load duration and by confidence.

1. **Cut round trips per record: bulk the alias and child-row writes.** *Highest expected
   gain, medium confidence.* Experiment B shows the cost is waits, not commits. Per record
   the loader makes 6 to 9 dependent round trips: resolve, mint, attach, upsert, media
   (4 statements), identifier attach, relationships, links. Candidates: resolve all aliases
   for a record (MBID, Discogs id, barcode, catalog numbers) in one `resolve_aliases` /
   `attach_aliases` call, the helpers already accept a batch; send the media statements
   pipelined; put relationships and links in a single pipeline. The long-run version is the
   Discogs-style per-entity batch, which amortizes everything across many records. Needs a
   design decision because the identity ledger's per-message transaction semantics
   (rollback with the row) would have to survive batching.
2. **Stop rewriting unchanged relationship and link rows.** *High confidence on the
   mechanism, needs a design decision.* Reruns update every row and defeat HOT. The
   reconciliation purge needs `updated_at` to prove "seen this run", so dropping the refresh
   is not an option on its own. Alternatives to evaluate: a per-run marker on a narrow side
   table keyed by source MBID, or one `UPDATE ... SET updated_at` per source MBID instead of
   per row, with `WHERE ... IS DISTINCT FROM` on the real columns. This changes what the
   purge relies on, so it must be agreed with `database-schema` and with the purge in
   `_reconciliation.py`. Per the repo rule on real fixes: the goal is that reconciliation
   still removes dropped rows, not merely that the load stops writing.
3. **Review the index set on `relationships`.** *Medium confidence, schema-owned.* Eight
   indexes per row insert, `idx_mb_rels_type` is low-cardinality and probably not worth its
   write cost, and two endpoint indexes overlap `idx_mb_rels_source` and `_target`. This is a
   `database-schema` change, and its readers (the `graph.mb_rel_*` views) decide which are
   needed. File against that repo; do not drop anything here.
4. **Coordinate the natural-key shape with `gm-database-schema-bmxc`.** *Needs input from
   that bead.* `relationships_natural_key` is `UNIQUE NULLS NOT DISTINCT` over eight columns
   including `attributes jsonb`. The conflict target in `_persistence.py` mirrors it
   exactly, so any key change is a coordinated change across both repositories. A narrower
   key (for example a hash of the distinguishing columns) would shrink the largest index and
   speed conflict detection. I could not read `gm-database-schema-bmxc` from this worktree
   (`bd` is not available here), so its current proposal is not reflected.
5. **Stop storing relations and links twice.** *Medium confidence on size, low on effect on
   speed.* `data` receives the whole record, including `relations` and `external_links`, and
   those are also written to their tables. That inflates heap and WAL for artists and
   releases. Dropping the keys from `data` would change what readers of `data` see, so it
   needs a schema and consumer check first.
6. **Tune for the load phase, not the loader.** *Low risk, outside this repo.* Deployment
   settings for the first load (`max_wal_size`, `checkpoint_timeout`, `shared_buffers`
   sized against the `relationships` index set, `synchronous_commit` for the initial load
   only) belong to `deployment`.

Not worth doing: the `old` CTE and `identity_link_events` insert in each entity upsert cost
0.1 to 0.3 ms and almost never write; the cost is in the extra round trips around them.

## Full-load duration estimate

Lower bound, from run C (247 rec/s) and run A (100 rec/s), applied to the extraction's
record counts. Labels and release groups are already complete on lux, so they are excluded
from the remaining count.

| Scope | Records | At 247 rec/s | At 100 rec/s |
| --- | --- | --- | --- |
| Remaining on lux today: 15.6M releases + 2.4M artists | 18.0M | about 20 h | about 50 h |
| Full load from empty: 19.6M releases + 10.3M artists | 29.9M | about 34 h | about 83 h |

How each figure should scale with the real row counts:

- **Linear in records** to first order, then worse. Rate falls as `relationships`, its eight
  indexes, `provider_aliases` and `catalog_items` outgrow `shared_buffers`. Treat the numbers
  as a floor and expect the real run to be slower, not faster.
- **Linear in relationships per record**, which I assumed (14 per release, 25 per artist).
  Real MusicBrainz artists in particular can carry hundreds. If real averages are double,
  expect the relationship statements (the largest server cost here) to roughly double.
- **Disk** is about 500 B per relationship (heap plus indexes) plus the entity row and its
  doubled `data`. Size the volume from the real relationship count before the first load.
- **A rerun costs about the same as a first load** (run D), so a re-extraction after this
  load will need the same hours again until finding 2 is addressed.
- Latency to lux Postgres is not in these numbers. At 7 dependent round trips per record,
  each extra millisecond of network latency adds about 7 ms to every record's time on its
  connection.

This estimate should be replaced by a measured run once the Postgres load completes and
`gm-deployment-vc5s` plans the first full MusicBrainz load.

## Proposed follow-up beads (not filed; the bead says nothing is implemented here)

1. `musicbrainz-sql-loader`: cut round trips per record by bulking alias resolution and
   pipelining media and child-row writes (finding 1). Acceptance: record rate, measured with
   the same harness shape, improves with identical resulting rows.
2. `musicbrainz-sql-loader` plus `database-schema`: stop rewriting unchanged relationships
   and links while keeping delete reconciliation correct (finding 2).
3. `database-schema`: review the eight indexes on `musicbrainz.relationships`
   (finding 3), and the natural-key width with `gm-database-schema-bmxc` (finding 4).
4. `musicbrainz-sql-loader` plus `database-schema`: stop duplicating `relations` and
   `external_links` inside `data` (finding 5).
5. `deployment`: first-load Postgres settings and volume sizing for MusicBrainz
   (finding 6), and a measured full-load run to replace this estimate.
6. `musicbrainz-sql-loader`: an integration-lane benchmark so this measurement is repeatable
   (the harness used here is not in the repo).
