"""Write-path benchmark: drive the real record processor into a real, disposable PostgreSQL.

Reproduces the runs in docs/write-path-throughput-audit-2026-11-04.md and the before/after
numbers of the beads that followed it. Run it through ``just bench-write``, which starts the
database; this script only talks to the URL it is given.

What it measures: ``MusicBrainzRecordProcessor`` writing synthetic, MusicBrainz-shaped records
through the production writer, one transaction per record, as ``_handle_data_message`` does.
RabbitMQ, settlement and telemetry are not in the loop. The records are synthetic, because a real
sample needs production access; the shape is the audit's (per artist 25 relations and 6 links, per
release 12 and 3, per release group 3 and 2, per label 5 and 3; mix 60% releases, 25% artists,
10% release groups, 5% labels), so absolute numbers move with relationship volume.

Runs:
  sequential   one connection, one record at a time            (audit run A)
  concurrent   WORKERS connections                              (audit run C, the production shape)
  rerun        the same records again over a loaded database    (audit run D: every row conflicts)

It also prints a fingerprint of every table the loader writes, so a change that is meant to keep
results identical can be shown to. ``data`` is fingerprinted without ``relations`` and
``external_links``, which the entity tables stopped storing; the fingerprints are therefore
comparable across that change.

Safety: it writes real rows, so it refuses a database that is not on loopback and one that
already holds MusicBrainz rows.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import sys
import time
import uuid
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import psycopg
from common import AsyncPostgreSQLPool
from common.media import map_musicbrainz_release
from groovemap_schema.postgres import create_postgres_schema
from psycopg.conninfo import conninfo_to_dict

from brainztableinator._persistence import PostgreSQLMusicBrainzWriter
from brainztableinator._record_processing import MusicBrainzRecordProcessor


# (relations, links) per record, and the entity mix, from the audit.
SHAPE = {"releases": (12, 3), "artists": (25, 6), "release_groups": (3, 2), "labels": (5, 3)}
MIX = (("releases", 0.60), ("artists", 0.85), ("release_groups", 0.95), ("labels", 1.0))

FINGERPRINT_QUERIES = {
    "artists": "SELECT mbid::text, name, discogs_artist_id, aliases, tags, data - 'relations' - 'external_links' FROM musicbrainz.artists ORDER BY 1",
    "labels": "SELECT mbid::text, name, data - 'relations' - 'external_links' FROM musicbrainz.labels ORDER BY 1",
    "releases": "SELECT mbid::text, name, barcode, discogs_release_id, media, data - 'relations' - 'external_links' FROM musicbrainz.releases ORDER BY 1",
    "release_groups": "SELECT mbid::text, name, data - 'relations' - 'external_links' FROM musicbrainz.release_groups ORDER BY 1",
    "relationships": (
        "SELECT source_mbid::text, source_entity_type, target_mbid::text, target_entity_type, relationship_type, attributes, begin_date, end_date, ended "
        "FROM musicbrainz.relationships ORDER BY 1, 2, 3, 4, 5, 6::text, 7, 8"
    ),
    "external_links": "SELECT mbid::text, entity_type, url, service_name FROM musicbrainz.external_links ORDER BY 1, 2, 3, 4",
    "issued_on": "SELECT release_id, medium_id, source, qty FROM graph.issued_on ORDER BY 1, 2, 3",
    "provider_aliases": "SELECT provider, entity_kind, external_id FROM public.provider_aliases ORDER BY 1, 2, 3",
    "identity_link_events": "SELECT provider, entity_kind, external_id, event, discogs_id_before, discogs_id_after FROM public.identity_link_events ORDER BY 1, 2, 3, 4",
}


class _Observer:
    def flush(self, _entity: str) -> Any:
        return MagicMock()

    def record(self, *_args: Any) -> None:
        return None

    def set_outcome(self, *_args: Any) -> None:
        return None


def make_records(count: int, seed: int) -> list[tuple[str, dict[str, Any]]]:
    """Return ``count`` deterministic records of the audit's shape."""
    rng = random.Random(seed)  # noqa: S311 - a benchmark's data generator, not a secret

    def new_uuid() -> str:
        return str(uuid.UUID(int=rng.getrandbits(128), version=4))

    records: list[tuple[str, dict[str, Any]]] = []
    for i in range(count):
        draw = rng.random()
        kind = next(name for name, ceiling in MIX if draw < ceiling)
        relations, links = SHAPE[kind]
        mbid = new_uuid()
        record: dict[str, Any] = {
            "id": mbid,
            "mbid": mbid,
            "name": f"{kind} {i}",
            "dump_version": "bench",
            "relations": [
                {
                    "target_mbid": new_uuid(),
                    "target_type": rng.choice(["artist", "release", "label", "recording"]),
                    "type": rng.choice(["member of band", "producer", "remixer", "vocal"]),
                    "direction": rng.choice(["forward", "backward"]),
                    "attributes": [],
                    "begin_date": None,
                    "end_date": None,
                    "ended": False,
                }
                for _ in range(relations)
            ],
            "external_links": [
                {"url": f"https://example.org/{new_uuid()}", "service": rng.choice(["wikidata", "bandcamp", "discogs", "youtube"])}
                for _ in range(links)
            ],
        }
        if kind == "releases":
            record["barcode"] = str(rng.randrange(10**12, 10**13)) if rng.random() < 0.5 else None
            record["catalog_numbers"] = [{"catalog_number": f"CAT-{rng.randrange(10**6)}"}] if rng.random() < 0.5 else []
            record["status"] = "Official"
            if i % 3 == 0:  # a third of releases name a Discogs release, as in the audit
                record["discogs_release_id"] = str(1_000_000 + i)
            record["media"] = map_musicbrainz_release({"media": [{"format": "CD", "disc-count": 1}], "status": "Official"})
        elif kind == "artists" and i % 4 == 0:
            record["discogs_artist_id"] = 2_000_000 + i
        records.append((kind, record))
    return records


async def _delay_pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, delay: float) -> None:
    try:
        while data := await reader.read(65536):
            await asyncio.sleep(delay)
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()


async def start_latency_proxy(url: str, one_way_ms: float) -> tuple[asyncio.Server, str]:
    """Front the database with a TCP proxy that delays every chunk, to stand in for network latency."""
    parts = urlsplit(url)
    upstream = (parts.hostname or "127.0.0.1", parts.port or 5432)

    async def handle(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
        server_reader, server_writer = await asyncio.open_connection(*upstream)
        await asyncio.gather(
            _delay_pipe(client_reader, server_writer, one_way_ms / 1000),
            _delay_pipe(server_reader, client_writer, one_way_ms / 1000),
        )

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, parts._replace(
        netloc=f"{parts.netloc.rsplit('@', 1)[0]}@127.0.0.1:{port}" if "@" in parts.netloc else f"127.0.0.1:{port}"
    ).geturl()


async def run_records(url: str, records: list[tuple[str, dict[str, Any]]], workers: int) -> float:
    """Write every record, one transaction each, across ``workers`` connections; return seconds."""
    processor = MusicBrainzRecordProcessor(PostgreSQLMusicBrainzWriter(), _Observer(), map_musicbrainz_release)
    handlers = {
        "releases": processor.process_release,
        "artists": processor.process_artist,
        "release_groups": processor.process_release_group,
        "labels": processor.process_label,
    }
    queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()
    for item in records:
        queue.put_nowait(item)

    async def worker() -> None:
        async with await psycopg.AsyncConnection.connect(url) as conn:
            await conn.set_autocommit(False)
            while True:
                try:
                    kind, record = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                async with conn.transaction():
                    await handlers[kind](conn, record)

    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(workers)))
    return time.perf_counter() - started


async def fingerprints(url: str) -> dict[str, tuple[int, str]]:
    result: dict[str, tuple[int, str]] = {}
    async with await psycopg.AsyncConnection.connect(url) as conn:
        for name, query in FINGERPRINT_QUERIES.items():
            rows = await (await conn.execute(query)).fetchall()  # type: ignore[call-overload]
            result[name] = (len(rows), hashlib.sha256(json.dumps(rows, default=str, sort_keys=True).encode()).hexdigest()[:16])
    return result


async def storage(url: str) -> dict[str, tuple[int, int, int, int]]:
    """Per child table: inserts, updates, HOT updates, total bytes (heap, toast and indexes)."""
    out: dict[str, tuple[int, int, int, int]] = {}
    async with await psycopg.AsyncConnection.connect(url, autocommit=True) as conn:
        await asyncio.sleep(1.5)  # the statistics collector flushes asynchronously
        for table in ("artists", "labels", "releases", "release_groups", "relationships", "external_links"):
            row = await (
                await conn.execute(
                    "SELECT n_tup_ins, n_tup_upd, n_tup_hot_upd, pg_total_relation_size(relid) FROM pg_stat_user_tables WHERE schemaname = 'musicbrainz' AND relname = %s",
                    (table,),
                )
            ).fetchone()
            if row is not None:
                out[table] = (int(row[0]), int(row[1]), int(row[2]), int(row[3]))
    return out


async def prepare(url: str) -> None:
    host = urlsplit(url).hostname
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise SystemExit(f"refusing to benchmark against {host!r}: this writes real rows and only loopback databases are accepted")
    pool = AsyncPostgreSQLPool(connection_params=conninfo_to_dict(url), max_retries=1)
    await pool.initialize()
    try:
        failures = await create_postgres_schema(pool)
    finally:
        await pool.close()
    if failures:
        raise SystemExit(f"{failures} schema statements failed")
    async with await psycopg.AsyncConnection.connect(url) as conn:
        for table in ("artists", "labels", "releases", "release_groups", "relationships", "external_links"):
            row = await (await conn.execute(f"SELECT count(*) FROM musicbrainz.{table}")).fetchone()  # type: ignore[call-overload]  # noqa: S608 - fixed literals above
            if row and row[0]:
                raise SystemExit(f"refusing to benchmark: musicbrainz.{table} already holds {row[0]} rows; use an empty database")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else None)
    parser.add_argument("--records", type=int, default=int(os.environ.get("BENCH_RECORDS", "2000")))
    parser.add_argument("--workers", type=int, default=int(os.environ.get("BENCH_WORKERS", "12")))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("BENCH_SEED", "42")))
    parser.add_argument(
        "--latency-ms",
        type=float,
        default=float(os.environ.get("BENCH_LATENCY_MS", "0")),
        help="one-way delay a TCP proxy adds in front of the database",
    )
    parser.add_argument("--skip-sequential", action="store_true")
    args = parser.parse_args()

    url = os.environ.get("BENCH_DATABASE_URL") or os.environ.get("TEST_DATABASE_URL")
    if not url:
        raise SystemExit("set BENCH_DATABASE_URL (or run `just bench-write`, which starts a disposable database)")
    await prepare(url)

    proxy = None
    target = url
    if args.latency_ms > 0:
        proxy, target = await start_latency_proxy(url, args.latency_ms)

    records = make_records(args.records, args.seed)
    print(f"records={args.records} workers={args.workers} seed={args.seed} latency_ms={args.latency_ms}")
    if not args.skip_sequential:
        seconds = await run_records(target, records, 1)
        print(f"RATE sequential (1 connection)      {args.records / seconds:8.1f} rec/s  ({seconds:.1f} s)")
        # The concurrent run must start from the same empty state the sequential one did.
        async with await psycopg.AsyncConnection.connect(url, autocommit=True) as conn:
            await conn.execute(
                "TRUNCATE musicbrainz.artists, musicbrainz.labels, musicbrainz.releases, musicbrainz.release_groups, "
                "musicbrainz.relationships, musicbrainz.external_links, graph.issued_on, public.identity_link_events"
            )
            await conn.execute("DELETE FROM public.provider_aliases")
            await conn.execute("DELETE FROM public.catalog_items")
    seconds = await run_records(target, records, args.workers)
    print(f"RATE concurrent ({args.workers} connections)   {args.records / seconds:8.1f} rec/s  ({seconds:.1f} s)")
    seconds = await run_records(target, records, args.workers)
    print(f"RATE rerun, all rows conflict        {args.records / seconds:8.1f} rec/s  ({seconds:.1f} s)")

    for table, (ins, upd, hot, size) in (await storage(url)).items():
        print(f"STORAGE {table:15} inserts={ins} updates={upd} hot_updates={hot} total_bytes={size}")
    for name, (rows, digest) in (await fingerprints(url)).items():
        print(f"FINGERPRINT {name:21} rows={rows} sha256={digest}")

    if proxy is not None:
        proxy.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
