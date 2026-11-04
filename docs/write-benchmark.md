# Write benchmark

One command reproduces the write-path numbers in
[the 2026-11-04 throughput audit](write-path-throughput-audit-2026-11-04.md) and measures any change to
the write path against them:

```sh
just bench-write                          # 2,000 records, 12 workers, about 40 seconds
just bench-write --records 5000           # a bigger run
just bench-write --latency-ms 1           # add 1 ms each way between loader and database
```

It needs Docker (or a loopback database of your own in `BENCH_DATABASE_URL`) and starts a pinned
`postgres:18-alpine` on a random loopback port, removed with its volume when the run ends. It is
outside `just check`.

## What it measures

`MusicBrainzRecordProcessor` writing synthetic, MusicBrainz-shaped records through the production
writer, **one transaction per record**, as `_handle_data_message` does. RabbitMQ, settlement and
telemetry are not in the loop. The schema is `create_postgres_schema` from the pinned
`database-schema`.

| Output | Meaning |
| --- | --- |
| `RATE sequential` | One connection, one record at a time (audit run A). Most sensitive to round trips. |
| `RATE concurrent` | `--workers` connections (default 12, audit run C): the production shape. |
| `RATE rerun` | The same records again over a loaded database, so every row conflicts (audit run D). |
| `STORAGE` | Per table: inserts, updates, HOT updates and total bytes (heap, TOAST, indexes). Counters are cumulative over the runs. |
| `FINGERPRINT` | Row count and hash of everything the loader writes. Use it to show a change keeps results identical. |

The records are the audit's shape (per artist 25 relations and 6 links, per release 12 and 3, per
release group 3 and 2, per label 5 and 3; mix 60% releases, 25% artists, 10% release groups, 5%
labels; a third of releases and a quarter of artists name a Discogs id). They are generated from
`--seed` (default 42), so two runs write the same rows. The fingerprint of `data` leaves out
`relations` and `external_links`, which the entity tables no longer store, so fingerprints compare
across that change.

## How to read it

- **Compare two builds on the same machine, back to back.** Run it on the base, then on the change,
  and compare the `RATE` lines and the `FINGERPRINT` lines. A shared or busy machine moves the
  concurrent numbers by tens of percent; the sequential and `--latency-ms` runs are steadier.
- **The numbers are a ceiling.** The tables start empty, the database is on loopback and nothing else
  writes to it. Real runs add btree depth, cache misses, checkpoints and network latency.
- **Loopback hides round trips.** With 12 workers on loopback the loader is limited by CPU and the
  server, not by waiting, so a change that removes waits barely moves it. `--latency-ms` runs a TCP
  proxy that delays every chunk, which is what a change in round trips is worth over a real network.
- **The records are synthetic.** A real sample needs production access. Relationship volume per
  record is the dominant unknown, so absolute rates move with it.
- **It refuses a database that is not on loopback, or one that already holds MusicBrainz rows**,
  because it writes real rows.

## Reference results

Measured on a developer Mac (Docker Desktop, `postgres:18-alpine`), 2,000 records, 12 workers. They
show the shape of the output, not a promise; compare your own runs against your own base.

| Change | Sequential | Sequential, 1 ms | 12 workers, 1 ms | Storage |
| --- | --- | --- | --- | --- |
| Before the round-trip work | 106 rec/s | 25.1 rec/s (300 records) | 240 rec/s (1,000 records) | |
| One pipeline per record | 145 rec/s | 35.5 rec/s | 289 rec/s | |
| `data` without `relations` and `external_links` | unchanged | unchanged | unchanged | entity tables 10.56 MB to 3.89 MB |

Fingerprints were identical across every row of that table.
