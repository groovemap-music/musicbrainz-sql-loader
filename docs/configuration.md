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
| `MUSICBRAINZ_EXCHANGE_PREFIX` | `groovemap-musicbrainz` | Fanout exchange prefix |
| `POSTGRES_POOL_MIN_SIZE` | `2` | Minimum PostgreSQL pool size |
| `POSTGRES_POOL_MAX_SIZE` | `12` | Maximum pool size and channel-global AMQP prefetch |
| `CONSUMER_CANCEL_DELAY` | `300` | Seconds between `file_complete` and consumer cancellation; `0` disables cancellation |
| `QUEUE_CHECK_INTERVAL` | `3600` | Seconds between durable-queue checks while idle |
| `STUCK_CHECK_INTERVAL` | `30` | Seconds between missing-consumer recovery checks |
| `STARTUP_IDLE_TIMEOUT` | `30` | Seconds without messages before idle mode |
| `IDLE_LOG_INTERVAL` | `300` | Seconds between idle status logs |
| `STARTUP_DELAY` | `5` | Seconds to allow dependencies to start |

Pool sizes are clamped so `1 <= min <= max`. RabbitMQ prefetch is tied to the maximum
pool size because every in-flight delivery can hold one PostgreSQL connection.

## Runtime endpoints

The health server always listens on port `8010`. A typical response includes the service
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
