# Changelog

All notable changes to this repository will be recorded here by Commitizen from
Conventional Commits.

## v0.7.0 (2026-11-09)

### Release notes

The loader starts in shadow mode when `loader_seen` exists (a startup probe; otherwise it falls back to touch mode, so restart it after the schema is applied). Shadow keeps today's touch and purge and logs the seen-list comparison at each purge. Seen mode needs operator approval after a matching shadow run. Needs database-schema 0.13.0 for shadow and seen; on older schemas it runs in touch mode.

### Feat

- **reconciliation**: stop rewriting unchanged rows on a reload, with seen lists

### Fix

- **telemetry**: count release alias conflicts instead of warning on each one
- **release**: keep breaking changes below 1.0 with major_version_zero

## v0.6.1 (2026-11-07)

### Release notes

Concurrent messages for one release no longer race on `graph.issued_on`: the prune and insert run under a transaction-scoped advisory lock, so neither message dead-letters. Replaying messages parked by the older race is safe. No schema change.

### Fix

- **persistence**: serialize concurrent writers of one release's issued_on range

## v0.6.0 (2026-11-06)

### Release notes

Needs the database-schema release that carries `purge_vetoed`: deploy the schema first. Relations and external links are no longer stored in `data`, and each record's writes are pipelined into one round trip.

### Feat

- **purge**: keep the veto durable and alert on consecutive vetoes
- **bench**: add a repeatable write-path benchmark

### Fix

- **persistence**: target the hashed relationships key in ON CONFLICT
- **delivery**: let a replayed signal recover the run and run the proof in CI
- **delivery**: pace every requeue and bound a message that keeps failing
- **bench**: compose the emptiness check with psycopg.sql identifiers
- **musicbrainz**: quarantine SQLSTATE class 54 instead of requeueing forever

### Perf

- **musicbrainz**: stop storing relations and external_links in data
- **musicbrainz**: pipeline each record's writes into one round trip

## v0.5.0 (2026-10-31)

### Feat

- **health**: serve /health on role port 8002 with HEALTH_PORT override
- **persistence**: record Discogs link changes at upsert time

### Fix

- **logging**: pin runtime with LOG_FILE override and console fallback
- **ingestion**: share pool admission and drain MusicBrainz callbacks (#17)
- **consumer**: bound writes with shared admission
- **reconciliation**: requeue without admission
- **ingestion**: reconcile MusicBrainz write admission lifecycle
- **deps**: upgrade msgpack past CVE-2026-114613
- **ingestion**: bound MusicBrainz writers with shared admission
- **runtime**: exit 1 when brainztableinator startup gives up
- **deps**: upgrade filelock to 4.2.1 past CVE-2026-112940
- **deps**: update certifi to 2026.10.19 and opentelemetry-api to 1.45.1
- **deps**: update multidict to 6.9.1 (#10)

## v0.4.0 (2026-10-04)

### Feat

- **identifiers**: consume producer-owned MusicBrainz release aliases
- **graph**: write the MusicBrainz half of graph.issued_on
- **reconcile**: latch the purge on extraction_complete and dead letters
- **reconcile**: add the stale child-row purge for the MusicBrainz tables

### Fix

- **deps**: update urllib3 to 2.8.0
- **consumers**: bound confirmed cancellation and preserve recovery state
- **consumers**: detect silent stream starvation
- **reconcile**: veto the purge before the rejected delivery is settled
- **reconcile**: settle a vetoed boundary and require an aware column type
- **persistence**: refresh updated_at on every child-row conflict

### Refactor

- **reconcile**: probe for the column and take the boundary from the message

## v0.3.0 (2026-09-15)

### Feat

- **loader**: attach barcode and catalogue-number aliases per release message
- **loader**: attach or mint native catalog ids per message and write gm_item_id
- **telemetry**: trace deliveries and batch flushes, and sample event-loop lag

### Fix

- **contracts**: enforce persistence provenance
- **ci**: accept commitizen's no-eligible-commits bump-preview state
- **deps**: bump python to 3.14.7-slim and dockerfile frontend to 1.27 (#1)

### Refactor

- **delivery**: centralize MusicBrainz SQL settlement
- **sql**: extract MusicBrainz persistence seams

## v0.2.0 (2026-09-04)

### Feat

- **loader**: write the canonical media block on release upsert
- **telemetry**: adopt common.telemetry and record pipeline metrics

### Fix

- **ci**: use public python libraries

## v0.1.1 (2026-08-31)

### Fix

- **ci**: accept release-boundary bump states and use Commitizen's supported files-only option

## v0.1.0 (2026-08-31)

The v0.1.0 workflow failed before publishing artifacts or images. The tag is
retained as an immutable record of that release attempt.
