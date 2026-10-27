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
| `POSTGRES_POOL_MAX_SIZE` | `12` | Maximum pool size and channel-global AMQP prefetch |
| `CONSUMER_CANCEL_DELAY` | `300` | Seconds between a completion marker and consumer cancellation; `0` disables cancellation |
| `QUEUE_CHECK_INTERVAL` | `3600` | Seconds between durable-queue checks while idle |
| `STUCK_CHECK_INTERVAL` | `30` | Seconds between missing-consumer recovery checks |
| `HEALTH_PORT` | `8002` | Port for the `/health` endpoint; invalid values fail startup |
| `STARTUP_IDLE_TIMEOUT` | `30` | Seconds without messages before idle mode |
| `IDLE_LOG_INTERVAL` | `300` | Seconds between idle status logs |
| `STARTUP_DELAY` | `5` | Seconds to allow dependencies to start |

Pool sizes are clamped so `1 <= min <= max`. RabbitMQ prefetch is tied to the maximum
pool size because every in-flight delivery can hold one PostgreSQL connection.

The promoted [catalog-event contract](../contracts/catalog-events/v1/contract.json) is the
authority for entity vocabulary, exchange names, queue templates, and producer provenance.
Deployment-specific values and secret mounting belong to the
[`deployment` configuration](https://github.com/groovemap-music/deployment/blob/main/docs/configuration.md).

## Logging

The service writes JSON logs to stdout and to `/logs/musicbrainz-sql-loader.log` via
`groovemap-runtime`'s `setup_logging`. The file sink is a size-capped rotating handler, not
an unbounded append-only file.

| Variable | Default | Description |
| --- | ---: | --- |
| `LOG_LEVEL` | `INFO` | Log verbosity; an unrecognized value falls back to `INFO` |
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
