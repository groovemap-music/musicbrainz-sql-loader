# Configuration reference

`musicbrainz-sql-loader` reads configuration from environment variables. Variables with
a `_FILE` form read the value from that path and take precedence over the plain variable.

## Required connections

| Variable | `_FILE` supported | Description |
| --- | --- | --- |
| `POSTGRES_HOST` | No | PostgreSQL host, optionally with a port |
| `POSTGRES_USERNAME` | Yes | PostgreSQL user |
| `POSTGRES_PASSWORD` | Yes | PostgreSQL password |
| `POSTGRES_DATABASE` | No | Database containing the `musicbrainz` schema |
| `RABBITMQ_USERNAME` | Yes | RabbitMQ user |
| `RABBITMQ_PASSWORD` | Yes | RabbitMQ password |

`POSTGRES_PORT` defaults to `5432` unless `POSTGRES_HOST` includes a port.
`RABBITMQ_HOST` defaults to `rabbitmq`, and `RABBITMQ_PORT` defaults to `5672`.
Credentials have no defaults.

## Loader behavior

| Variable | Default | Description |
| --- | ---: | --- |
| `MUSICBRAINZ_EXCHANGE_PREFIX` | `groovemap-musicbrainz` | Fanout exchange prefix from the promoted producer contract |
| `POSTGRES_POOL_MIN_SIZE` | `2` | Minimum PostgreSQL pool size |
| `POSTGRES_POOL_MAX_SIZE` | `12` | Maximum pool size, shared database-writer bound, and per-consumer AMQP delivery limit |
| `CONSUMER_CANCEL_DELAY` | `300` | Seconds between a completion marker and consumer cancellation; `0` disables cancellation |
| `QUEUE_CHECK_INTERVAL` | `3600` | Seconds between durable-queue checks while idle |
| `STUCK_CHECK_INTERVAL` | `30` | Seconds between missing-consumer recovery checks |
| `HEALTH_PORT` | `8002` | Port for the `/health` endpoint; invalid values fail startup |
| `STARTUP_IDLE_TIMEOUT` | `30` | Seconds without messages before idle mode |
| `IDLE_LOG_INTERVAL` | `300` | Seconds between idle status logs |
| `STARTUP_DELAY` | `5` | Seconds to allow dependencies to start |
| `RELOAD_MODE` | `shadow` | How a reload proves a child row is still upstream: `touch`, `shadow` or `seen` (see below); any other value fails startup |

Pool sizes are clamped so `1 <= min <= max`. Each RabbitMQ consumer's prefetch is tied to
the maximum pool size; one shared admission budget caps actual PostgreSQL writers across
all entity consumers and reconciliation work.

## Reload mode

A monthly reload re-sends every record, and the stale-row purge deletes the
`musicbrainz.relationships` and `musicbrainz.external_links` rows the reload did not reach.
`RELOAD_MODE` chooses how the loader records that a reload reached a row. The shared design is
database-schema's [Reload seen lists](https://github.com/groovemap-music/database-schema/blob/main/docs/architecture.md#reload-seen-lists).

| Mode | Child-row writes | Purge |
| --- | --- | --- |
| `touch` | Every conflict rewrites the row to refresh `updated_at`, changed or not | Deletes rows whose `updated_at` predates the extraction |
| `shadow` | As `touch`, and appends each row's key to its `loader_seen` list | As `touch`; logs `loader_seen.<list>_shadow` for both lists first, then restarts both lists |
| `seen` | Writes only a new row or a changed `ended`; appends each row's key | Deletes `loader_seen.<list>_stale` by id, under the delete-fraction cap; restarts both lists |

At startup the loader probes for the `loader_seen` schema, its lists and functions, and
`musicbrainz.external_link_key`. If any is missing it runs in `touch` mode whatever `RELOAD_MODE`
says, and logs a warning, so this release can ship before the database-schema release that adds
them. The entity upserts skip an unchanged row in every mode: the entity tables are not purged.

Rollout and rollback:

1. Deploy the database-schema release carrying `loader_seen`. The loader then runs `shadow`.
2. Let one full MusicBrainz reload settle. Read the two `Seen-list shadow comparison` log lines
   from its purge. The switch needs `intact`, `refreshed_not_seen = 0` and
   `touch_purge = seen_purge` on both, with every `seen_not_refreshed` row explained.
3. With operator approval, set `RELOAD_MODE=seen`. Never set it without a matching shadow reload.
4. To roll back, set `RELOAD_MODE=touch`. The first purge after switching back must follow a
   reload that ran with the touch: a reload without it leaves every unchanged row looking stale,
   and the delete-fraction cap is then all that stands between it and the table.

In `seen` mode, a list that is not intact for the extraction (emptied by a PostgreSQL crash, or
started after the extraction began) skips the purge, records the list in `purge_vetoed` like a
dead letter, and restarts it. See [Purge vetoes](database-resilience.md#purge-vetoes).

To run the seen-list integration tests before the persistence pin carries `loader_seen`, install a
database-schema checkout that does into the environment and run without syncing:
`uv pip install --no-deps <database-schema checkout>`, then
`TEST_DATABASE_URL=... uv run --no-sync pytest tests/integration/test_reload_seen_lists.py`, then
`uv sync --frozen` to restore the pin. Against the pinned schema those tests skip.

The promoted [catalog-event contract](../contracts/catalog-events/v1/contract.json) is the
authority for entity vocabulary, exchange names, queue templates, and producer provenance.
Deployment-specific values and secret mounting belong to the
[`deployment` configuration](https://github.com/groovemap-music/deployment/blob/main/docs/configuration.md).

## Logging

The service writes JSON logs to stdout and, by default, to `/logs/musicbrainz-sql-loader.log`
(the path containers mount) via `groovemap-runtime`'s `setup_logging`. The file sink is a
size-capped rotating handler, not an unbounded append-only file. `/logs` is not required
locally: if the file cannot be opened, the service logs one warning and continues on the
console only.

| Variable | Default | Description |
| --- | ---: | --- |
| `LOG_LEVEL` | `INFO` | Log verbosity; an unrecognized value falls back to `INFO` |
| `LOG_FILE` | `/logs/musicbrainz-sql-loader.log` | Log file path; empty disables file logging |
| `LOG_FILE_MAX_BYTES` | `104857600` (100 MiB) | Size the active log file rolls over at |
| `LOG_FILE_BACKUP_COUNT` | `5` | Number of rotated backup files retained |
| `ENVIRONMENT` | `development` | Value of the `environment` field on every log entry; does not change verbosity |

A non-numeric or non-positive override for either rotation variable falls back to its
default rather than disabling rotation.

The OpenTelemetry variables are listed in the README's
[Telemetry](../README.md#telemetry) section.

## Runtime endpoints

The health server listens on port `8002`, the role port shared by every SQL loader. Set `HEALTH_PORT` to
any port from `1` to `65535` to override it; the Docker `HEALTHCHECK` honours the same variable. A
non-numeric or out-of-range value is a configuration error and the service stops at startup instead of
falling back to the default. A typical response includes the service
identity, status, current task, per-stream message counts, active consumers, and completed
streams.

```json
{
  "status": "healthy",
  "service": "musicbrainz-sql-loader",
  "active_consumers": ["artists", "labels"],
  "completed_files": ["release-groups", "releases"]
}
```
