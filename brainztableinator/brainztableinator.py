import asyncio
import contextlib
import os
import signal
import time
import uuid
from asyncio import run
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

import structlog
from aio_pika.abc import AbstractIncomingMessage  # noqa: TC002 - runtime annotation introspection
from common import (
    AsyncPostgreSQLPool,
    AsyncResilientRabbitMQ,
    DatabaseUnavailableError,
    DeliveryResult,
    FailureKind,
    HealthServer,
    OutageBackoff,
    Settlement,
    extract_context,
    flush_span,
    get_meter,
    get_tracer,
    map_musicbrainz_release,
    parse_postgres_host_port,
    run_delivery,
    setup_logging,
    setup_telemetry,
    shutdown_telemetry,
    start_event_loop_monitor,
)
from opentelemetry.trace import SpanKind, Status, StatusCode
from orjson import loads
from psycopg.errors import DataError, IntegrityError, InterfaceError, OperationalError

from brainztableinator._persistence import PostgreSQLMusicBrainzWriter
from brainztableinator._reconciliation import StaleChildRowPurge, reconciliation_columns_present
from brainztableinator._record_processing import MusicBrainzRecordProcessor
from brainztableinator.config import MusicBrainzSQLLoaderConfig
from brainztableinator.queue_names import (
    AMQP_EXCHANGE_TYPE,
    CONSUMER_SOURCES,
    MUSICBRAINZ_DATA_TYPES,
)
from brainztableinator.queue_names import (
    dead_letter_exchange_name as catalog_dead_letter_exchange_name,
)
from brainztableinator.queue_names import (
    dead_letter_queue_name as catalog_dead_letter_queue_name,
)
from brainztableinator.queue_names import (
    exchange_name as catalog_exchange_name,
)
from brainztableinator.queue_names import (
    queue_name as catalog_queue_name,
)


logger = structlog.get_logger(__name__)

SERVICE_NAME = "musicbrainz-sql-loader"
# The queue suffix is a durable broker identifier used by deployed installations.
# Changing it would create new queues and strand messages in the existing queues.
AMQP_CONSUMER_ID = "brainztableinator"
STARTUP_BANNER = r"""
+--------------------------------+
| GrooveMap                      |
| musicbrainz-sql-loader         |
+--------------------------------+
""".strip("\n")

# One meter for this service, resolved lazily (see ``_instrument``) so the first real call
# always lands after ``main`` has called ``setup_telemetry`` — before that, a message cannot
# reach ``on_data_message`` and a batch cannot reach ``_insert_relationships`` /
# ``_insert_external_links``, because no consumer is registered until after telemetry setup.
METRICS_SCOPE = "groovemap.brainztableinator"

PIPELINE_SOURCE = CONSUMER_SOURCES[AMQP_CONSUMER_ID]["source"]

PIPELINE_MESSAGES = "groovemap.pipeline.messages"
PIPELINE_MESSAGE_DURATION = "groovemap.pipeline.message.duration"
PIPELINE_BATCH_SIZE = "groovemap.pipeline.batch.size"
PIPELINE_BATCH_FLUSH_DURATION = "groovemap.pipeline.batch.flush.duration"
PIPELINE_CONSUMERS_ACTIVE = "groovemap.pipeline.consumers.active"

# The shared delivery runner delegates observation to this service, so these instruments keep
# their established names and attributes while terminal settlement stays centralized.
MESSAGING_CONSUMED_MESSAGES = "messaging.client.consumed.messages"
MESSAGING_OPERATION_DURATION = "messaging.client.operation.duration"
MESSAGING_SYSTEM = "rabbitmq"

STORE = "postgresql"

# Metrics use singular entity names even though queue data types are plural.
_ENTITY_LABELS = {
    "artists": "artist",
    "labels": "label",
    "release-groups": "release-group",
    "releases": "release",
}

_instruments_lock = Lock()
_instruments: dict[str, Any] | None = None


def _build_instruments() -> dict[str, Any]:
    """Create every domain instrument from the currently installed meter provider."""
    meter = get_meter(METRICS_SCOPE)
    return {
        PIPELINE_MESSAGES: meter.create_counter(
            PIPELINE_MESSAGES,
            description="MusicBrainz pipeline messages settled (acked or nacked).",
        ),
        PIPELINE_MESSAGE_DURATION: meter.create_histogram(
            PIPELINE_MESSAGE_DURATION,
            unit="s",
            description="Duration of handling one MusicBrainz pipeline message.",
        ),
        PIPELINE_BATCH_SIZE: meter.create_histogram(
            PIPELINE_BATCH_SIZE,
            unit="{items}",
            description="Records flushed to PostgreSQL in one relationship/external-link batch.",
        ),
        PIPELINE_BATCH_FLUSH_DURATION: meter.create_histogram(
            PIPELINE_BATCH_FLUSH_DURATION,
            unit="s",
            description="Duration of flushing one relationship/external-link batch to PostgreSQL.",
        ),
        PIPELINE_CONSUMERS_ACTIVE: meter.create_up_down_counter(
            PIPELINE_CONSUMERS_ACTIVE,
            description="Active MusicBrainz pipeline consumers.",
        ),
        MESSAGING_CONSUMED_MESSAGES: meter.create_counter(
            MESSAGING_CONSUMED_MESSAGES,
            description="Messages consumed from the broker.",
        ),
        MESSAGING_OPERATION_DURATION: meter.create_histogram(
            MESSAGING_OPERATION_DURATION,
            unit="s",
            description="Duration of a messaging client operation.",
        ),
    }


def _instrument(name: str) -> Any:
    """Return one cached OpenTelemetry instrument, building the cache on first use."""
    global _instruments
    with _instruments_lock:
        if _instruments is None:
            _instruments = _build_instruments()
        return _instruments[name]


def reset_metric_instruments() -> None:
    """Drop the cached instruments so the next use rebuilds them. Test seam only."""
    global _instruments
    with _instruments_lock:
        _instruments = None


def _record_pipeline_message(entity: str, outcome: str, duration_s: float) -> None:
    """Record one settled pipeline message against the shared molecule conventions."""
    try:
        _instrument(PIPELINE_MESSAGES).add(1, {"source": PIPELINE_SOURCE, "entity": entity, "outcome": outcome})
        _instrument(PIPELINE_MESSAGE_DURATION).record(duration_s, {"source": PIPELINE_SOURCE, "entity": entity})
    except Exception:
        logger.debug("Could not record pipeline message metrics", exc_info=True)


def _record_consumed_message(destination: str, duration_s: float, error_type: str | None) -> None:
    """Record one consumed message through the shared delivery observer."""
    attributes: dict[str, str] = {
        "messaging.system": MESSAGING_SYSTEM,
        "messaging.destination.name": destination,
        "messaging.operation.name": "process",
    }
    if error_type is not None:
        attributes["error.type"] = error_type
    try:
        _instrument(MESSAGING_CONSUMED_MESSAGES).add(1, attributes)
        _instrument(MESSAGING_OPERATION_DURATION).record(duration_s, attributes)
    except Exception:
        logger.debug("Could not record consumed-message metrics", exc_info=True)


def _record_batch_flush(entity: str, size: int, duration_s: float, outcome: str) -> None:
    """Record one relationship/external-link batch flush to PostgreSQL."""
    attributes = {"store": STORE, "entity": entity, "outcome": outcome}
    try:
        _instrument(PIPELINE_BATCH_SIZE).record(size, attributes)
        _instrument(PIPELINE_BATCH_FLUSH_DURATION).record(duration_s, attributes)
    except Exception:
        logger.debug("Could not record batch flush metrics", exc_info=True)


def _record_consumer_delta(delta: int) -> None:
    """Adjust the active-consumer gauge by ``delta`` (+1 on start, -1 on stop)."""
    try:
        _instrument(PIPELINE_CONSUMERS_ACTIVE).add(delta, {"source": PIPELINE_SOURCE})
    except Exception:
        logger.debug("Could not record consumer count metric", exc_info=True)


# Spans report under the same instrumentation scope as the metrics above, so an operator
# reads one scope name for both signals. The tracer is resolved per span rather than cached
# at import time: ``main`` installs the TracerProvider inside ``setup_telemetry``, and a
# tracer captured before that would keep pointing at whatever was installed then.
#
# Every span opened here sets ``record_exception=False`` / ``set_status_on_exception=False``
# and marks a failure with a status plus ``error.type`` by hand, per the GrooveMap span
# conventions: no message, no stack trace, no span event carrying a payload. The shared
# helpers in ``common.tracing`` already do the same, so nesting reads consistently.

# The current delivery context lets private batch processing link its flush span without
# coupling record mapping or persistence to OpenTelemetry.
_message_span_context: ContextVar[Any | None] = ContextVar("brainztableinator_message_span", default=None)


def _mark_span_failed(span: Any, error_type: str) -> None:
    """Fail a span with ``error.type`` only. Never raises."""
    if span is None:
        return
    try:
        span.set_attribute("error.type", error_type)
        span.set_status(Status(StatusCode.ERROR))
    except Exception:
        logger.debug("Could not mark a span as failed", exc_info=True)


def _set_span_outcome(span: Any, outcome: str) -> None:
    """Record the closed-set outcome on a span. Never raises."""
    if span is None:
        return
    try:
        span.set_attribute("outcome", outcome)
    except Exception:
        logger.debug("Could not record a span outcome", exc_info=True)


@contextmanager
def _consume_span(destination: str, headers: Mapping[str, Any] | None) -> Iterator[Any]:
    """Open the CONSUMER span for one delivery: ``process {destination}``.

    The span joins the trace the extractor's publish started, which reaches this process as
    the ``traceparent`` header on the AMQP message; a delivery without a readable one starts
    a new trace rather than failing. The local delivery observer opens it from the same stable
    helpers used before shared settlement, preserving its name, kind, and attributes.

    Yields ``None`` when no span could be started, so the caller stays branch-free.
    """
    try:
        manager = get_tracer(METRICS_SCOPE).start_as_current_span(
            f"process {destination}",
            context=extract_context(headers) if headers else None,
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": MESSAGING_SYSTEM,
                "messaging.destination.name": destination,
                "messaging.operation.name": "process",
            },
            record_exception=False,
            set_status_on_exception=False,
        )
    except Exception:
        logger.debug("Could not start the consumer span", exc_info=True)
        yield None
        return

    with manager as span:
        yield span


def _span_context_of(span: Any) -> Any:
    """Return a span's context, or None when there is no recording span to link to.

    A no-op tracer yields the invalid span, whose context would otherwise become a link
    pointing nowhere. Never raises.
    """
    if span is None:
        return None
    try:
        context = span.get_span_context()
    except Exception:
        logger.debug("Could not read a span context", exc_info=True)
        return None
    return context if context is not None and context.is_valid else None


def _flush_links() -> list[Any]:
    """Return the message span contexts the batch flush about to run covers.

    One delivery drives one flush in this service, so this is a single-element list.
    ``common.flush_span`` applies the 64-link cap the conventions set, which is what bounds a
    consumer that instead flushes many deliveries in one batch.
    """
    context = _message_span_context.get()
    return [] if context is None else [context]


class _RuntimeBatchObserver:
    """Bridge record-batch events into the service's telemetry lifecycle."""

    def flush(self, entity: str) -> Any:
        return flush_span(STORE, entity, links=_flush_links())

    def record(self, entity: str, size: int, duration_s: float, outcome: str) -> None:
        _record_batch_flush(entity, size, duration_s, outcome)

    def set_outcome(self, span: Any, outcome: str) -> None:
        _set_span_outcome(span, outcome)


# Held by name so the startup probe can turn the delete-reconciliation column's
# refresh on; the writer emits no reference to a column the schema has not declared.
_persistence_writer = PostgreSQLMusicBrainzWriter()

_record_processor = MusicBrainzRecordProcessor(
    _persistence_writer,
    _RuntimeBatchObserver(),
    map_musicbrainz_release,
)


@contextmanager
def _delivery_observation(destination: str, headers: object | None) -> Iterator[Any]:
    """Keep local delivery telemetry and child-row flush links around shared settlement."""
    mapping_headers = headers if isinstance(headers, Mapping) else None
    with _consume_span(destination, mapping_headers) as span:
        span_token = _message_span_context.set(_span_context_of(span))
        destination_token = _delivery_destination.set(destination)
        try:
            yield span
        finally:
            _delivery_destination.reset(destination_token)
            _message_span_context.reset(span_token)


class _DeliveryObserver:
    """Bridge the shared delivery contract to this service's existing telemetry."""

    def consume(self, destination: str, headers: object | None) -> Any:
        return _delivery_observation(destination, headers)

    def settled(self, *, entity: str, result: DeliveryResult, duration_s: float, span: Any) -> None:
        local_outcome = result.outcome if result.settlement is Settlement.ACK else "failed"
        _record_pipeline_message(entity, local_outcome, duration_s)
        _record_consumed_message(_delivery_destination.get(), duration_s, result.error_type)
        _set_span_outcome(span, local_outcome)
        if result.error_type is not None:
            _mark_span_failed(span, result.error_type)


_delivery_destination: ContextVar[str] = ContextVar("brainztableinator_delivery_destination", default="unknown")
_delivery_observer = _DeliveryObserver()


class _MusicBrainzFailureClassifier:
    """Classify persistence failures while retaining outage-only requeue throttling."""

    def __init__(self, data_type: str) -> None:
        self._data_type = data_type
        self._throttle = False

    def __call__(self, error: BaseException) -> FailureKind:
        if isinstance(error, (InterfaceError, OperationalError, DatabaseUnavailableError)):
            self._throttle = True
            logger.warning("⚠️ Database connection issue, will retry", error=str(error))
            return FailureKind.TRANSIENT
        if isinstance(error, (DataError, IntegrityError)):
            logger.error(
                "❌ Non-retryable data error, nacking without requeue",
                data_type=self._data_type,
                error=str(error),
            )
            return FailureKind.DETERMINISTIC
        logger.error("❌ Failed to process message", data_type=self._data_type, error=str(error))
        return FailureKind.TRANSIENT

    async def wait_before_requeue(self) -> None:
        if self._throttle:
            await outage_backoff.wait()


config: MusicBrainzSQLLoaderConfig | None = None

# Fallback prefetch when config is not yet loaded (matches MusicBrainzSQLLoaderConfig default).
_DEFAULT_POOL_MAX = 12


def _channel_prefetch() -> int:
    """RabbitMQ prefetch coupled to the PostgreSQL pool capacity.

    This loader opens one transaction per message, so every
    in-flight handler holds a pooled connection for the duration of its write. Bounding
    the channel's total unacked deliveries (channel-global QoS) to the pool's ``max``
    means the broker — not the pool's exhausted-wait retry loop — applies backpressure,
    so the pool is never oversubscribed and the shared PgBouncer budget is respected.
    """
    return config.postgres_pool_max_size if config is not None else _DEFAULT_POOL_MAX


message_counts = {"artists": 0, "labels": 0, "release-groups": 0, "releases": 0}
progress_interval = 100
last_message_time = {
    "artists": 0.0,
    "labels": 0.0,
    "release-groups": 0.0,
    "releases": 0.0,
}
completed_files: set[str] = set()

# Throttle requeues so an outage cannot exhaust the quorum queue's delivery budget.
outage_backoff = OutageBackoff(SERVICE_NAME)
current_task = None
current_progress = 0.0

consumer_tags: dict[str, str] = {}
consumer_cancel_tasks: dict[str, asyncio.Task[None]] = {}
queues: dict[str, Any] = {}
CONSUMER_CANCEL_DELAY = int(os.environ.get("CONSUMER_CANCEL_DELAY", "300"))

QUEUE_CHECK_INTERVAL = int(os.environ.get("QUEUE_CHECK_INTERVAL", "3600"))

STUCK_CHECK_INTERVAL = int(os.environ.get("STUCK_CHECK_INTERVAL", "30"))

STARTUP_IDLE_TIMEOUT = int(os.environ.get("STARTUP_IDLE_TIMEOUT", "30"))
IDLE_LOG_INTERVAL = int(os.environ.get("IDLE_LOG_INTERVAL", "300"))

idle_mode = False

connection_params: dict[str, Any] = {}

connection_pool: AsyncPostgreSQLPool | None = None

# Built in main() once the pool is up, because it needs both the pool and a run start
# read from the database clock. None until then, which is what makes every consumer of
# it a no-op in the unit suite and in any process that never reached a live database.
stale_row_purge: StaleChildRowPurge | None = None

rabbitmq_manager: Any = None  # Will hold AsyncResilientRabbitMQ instance
active_connection: Any = None  # Current active connection
active_channel: Any = None  # Current active channel
connection_check_task: asyncio.Task[None] | None = None  # Background task for periodic queue checks


def get_health_data() -> dict[str, Any]:
    """Get current health data for monitoring."""
    active_task = None
    current_time = time.time()

    for data_type, last_time in last_message_time.items():
        if last_time > 0 and (current_time - last_time) < 10:
            active_task = f"Processing {data_type}"
            break

    if active_task is None and len(consumer_tags) > 0:
        active_task = "Idle - waiting for messages"

    no_active_consumers = len(consumer_tags) == 0
    files_incomplete = len(completed_files) < len(MUSICBRAINZ_DATA_TYPES)
    has_processed_messages = any(count > 0 for count in message_counts.values())
    is_stuck = no_active_consumers and files_incomplete and has_processed_messages

    if is_stuck:
        active_task = "STUCK - consumers died, awaiting recovery"

    if connection_pool is None:
        if len(consumer_tags) == 0 and all(c == 0 for c in message_counts.values()):
            status = "starting"
            active_task = "Initializing PostgreSQL connection"
        else:
            status = "unhealthy"
    elif is_stuck:
        status = "unhealthy"
    else:
        status = "healthy"

    return {
        "status": status,
        "service": SERVICE_NAME,
        "current_task": active_task,
        "progress": current_progress,
        "message_counts": message_counts.copy(),
        "last_message_time": last_message_time.copy(),
        "active_consumers": list(consumer_tags.keys()),
        "completed_files": list(completed_files),
        "timestamp": datetime.now(UTC).isoformat(),
    }


shutdown_requested = False


def signal_handler(signum: int, _frame: Any) -> None:
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    logger.info("🛑 Received signal, initiating graceful shutdown...", signum=signum)
    shutdown_requested = True


def get_connection() -> Any:
    """Get a database connection from the pool."""
    if connection_pool is None:
        raise RuntimeError("Connection pool not initialized")

    return connection_pool.connection()


async def schedule_consumer_cancellation(data_type: str, queue: Any) -> None:
    """Schedule cancellation of a consumer after a delay."""

    async def cancel_after_delay() -> None:
        try:
            await asyncio.sleep(CONSUMER_CANCEL_DELAY)

            if data_type in consumer_tags:
                consumer_tag = consumer_tags[data_type]
                logger.info(
                    f"🔧 Canceling consumer for {data_type} after {CONSUMER_CANCEL_DELAY}s grace period",
                    data_type=data_type,
                    CONSUMER_CANCEL_DELAY=CONSUMER_CANCEL_DELAY,
                )

                await queue.cancel(consumer_tag, nowait=True)

                del consumer_tags[data_type]
                _record_consumer_delta(-1)

                logger.info(
                    f"✅ Consumer for {data_type} successfully canceled",
                    data_type=data_type,
                )

                if await check_all_consumers_idle():
                    logger.info("🔧 All consumers idle, closing RabbitMQ connection")
                    await close_rabbitmq_connection()
        except Exception as e:
            logger.error("❌ Failed to cancel consumer", data_type=data_type, error=str(e))
        finally:
            consumer_cancel_tasks.pop(data_type, None)

    if data_type in consumer_cancel_tasks:
        consumer_cancel_tasks[data_type].cancel()

    consumer_cancel_tasks[data_type] = asyncio.create_task(cancel_after_delay())


async def cancel_all_consumers() -> None:
    """Stop new deliveries at shutdown by cancelling every consumer.

    Shutdown previously had no deregistration phase at all: the flag flipped, the
    consumers stayed subscribed, and the per-message guard nacked whatever the
    broker kept pushing. Cancelling here closes the delivery tap BEFORE the
    seconds-long flush/teardown sequence, so nothing is redelivered into a
    service that is on its way out. Best-effort: teardown continues regardless.
    """
    for data_type, consumer_tag in list(consumer_tags.items()):
        queue = queues.get(data_type)
        if queue is None:
            consumer_tags.pop(data_type, None)
            _record_consumer_delta(-1)
            continue
        try:
            await queue.cancel(consumer_tag, nowait=True)
            consumer_tags.pop(data_type, None)
            _record_consumer_delta(-1)
        except Exception as e:
            logger.warning(
                "⚠️ Failed to cancel consumer during shutdown",
                data_type=data_type,
                error=str(e),
            )
    logger.info("✅ Consumers cancelled for shutdown")


async def close_rabbitmq_connection() -> None:
    """Close the RabbitMQ connection and channel when all consumers are idle."""
    global active_connection, active_channel

    try:
        if active_channel:
            try:
                await active_channel.close()
                logger.info("🔧 Closed RabbitMQ channel - all consumers idle")
            except Exception as e:
                logger.warning("⚠️ Error closing channel", error=str(e))
            active_channel = None

        if active_connection:
            try:
                await active_connection.close()
                logger.info("🔧 Closed RabbitMQ connection - all consumers idle")
            except Exception as e:
                logger.warning("⚠️ Error closing connection", error=str(e))
            active_connection = None

        logger.info(
            f"✅ RabbitMQ connection closed. Will check for new messages every {QUEUE_CHECK_INTERVAL}s",
            QUEUE_CHECK_INTERVAL=QUEUE_CHECK_INTERVAL,
        )
    except Exception as e:
        logger.error("❌ Error closing RabbitMQ connection", error=str(e))


async def check_all_consumers_idle() -> bool:
    """Check if all consumers are cancelled (idle) AND all files completed."""
    return len(consumer_tags) == 0 and len(MUSICBRAINZ_DATA_TYPES) == len(completed_files)


async def check_consumers_unexpectedly_dead() -> bool:
    """Check if consumers have died unexpectedly (no consumers but files not completed).

    This detects the stuck state where:
    - No consumers are active (consumer_tags is empty)
    - Not all files are completed (some work remains)
    - We've processed at least some messages (not just starting up)

    Returns:
        True if consumers appear to have died unexpectedly
    """
    no_active_consumers = len(consumer_tags) == 0
    files_incomplete = len(completed_files) < len(MUSICBRAINZ_DATA_TYPES)
    has_processed_messages = any(count > 0 for count in message_counts.values())

    return no_active_consumers and files_incomplete and has_processed_messages


async def periodic_queue_checker() -> None:
    """Periodically check queue health and recover from stuck states."""

    last_full_check = 0.0

    while not shutdown_requested:
        try:
            await asyncio.sleep(STUCK_CHECK_INTERVAL)

            current_time = time.time()

            if await check_consumers_unexpectedly_dead():
                logger.warning(
                    "⚠️ Detected stuck state: consumers died but files not completed. Attempting recovery...",
                    active_consumers=len(consumer_tags),
                    completed_files=list(completed_files),
                    message_counts=message_counts,
                )
                await _recover_consumers()
                continue

            time_since_last_check = current_time - last_full_check
            if time_since_last_check < QUEUE_CHECK_INTERVAL:
                continue

            if active_connection or len(consumer_tags) > 0:
                continue

            last_full_check = current_time
            logger.info("🔄 Checking all queues for new messages...")
            await _recover_consumers()

        except asyncio.CancelledError:
            logger.info("🛑 Queue checker task cancelled")
            break
        except Exception as e:
            logger.error("❌ Error in periodic queue checker", error=str(e))


async def _recover_consumers() -> None:
    """Recover consumers by reconnecting to RabbitMQ and restarting consumption."""
    global active_connection, active_channel, queues, idle_mode

    if active_connection:
        try:
            await active_connection.close()
        except Exception as e:
            logger.warning("⚠️ Error closing broken connection during recovery", error=str(e))
        active_connection = None
        active_channel = None

    try:
        temp_connection = await rabbitmq_manager.connect()
        temp_channel = await temp_connection.channel()
    except Exception as e:
        logger.error("❌ Failed to connect to RabbitMQ for recovery", error=str(e))
        return

    try:
        queues_with_messages = []
        for data_type in MUSICBRAINZ_DATA_TYPES:
            queue_name = catalog_queue_name(AMQP_CONSUMER_ID, data_type)

            declared_queue = await temp_channel.declare_queue(name=queue_name, passive=True)

            if declared_queue.declaration_result.message_count > 0:
                queues_with_messages.append((data_type, declared_queue.declaration_result.message_count))

        if queues_with_messages:
            total_messages = sum(count for _, count in queues_with_messages)
            logger.info(
                "📬 Found messages in queues, restarting consumers",
                queues=queues_with_messages,
                total_messages=total_messages,
            )

            active_connection = temp_connection
            active_channel = temp_channel

            await active_channel.set_qos(prefetch_count=_channel_prefetch(), global_=True)

            queues = {}
            for data_type in MUSICBRAINZ_DATA_TYPES:
                exchange_name = catalog_exchange_name(data_type)
                queue_name = catalog_queue_name(AMQP_CONSUMER_ID, data_type)
                dlx_name = catalog_dead_letter_exchange_name(AMQP_CONSUMER_ID, data_type)
                dlq_name = catalog_dead_letter_queue_name(AMQP_CONSUMER_ID, data_type)

                exchange = await active_channel.declare_exchange(
                    exchange_name,
                    AMQP_EXCHANGE_TYPE,
                    durable=True,
                    auto_delete=False,
                )

                dlx_exchange = await active_channel.declare_exchange(
                    dlx_name,
                    AMQP_EXCHANGE_TYPE,
                    durable=True,
                    auto_delete=False,
                )

                dlq = await active_channel.declare_queue(
                    auto_delete=False,
                    durable=True,
                    name=dlq_name,
                    arguments={"x-queue-type": "classic"},
                )
                await dlq.bind(dlx_exchange)

                queue_args = {
                    "x-queue-type": "quorum",
                    "x-dead-letter-exchange": dlx_name,
                    "x-delivery-limit": 20,
                }
                queue = await active_channel.declare_queue(
                    auto_delete=False,
                    durable=True,
                    name=queue_name,
                    arguments=queue_args,
                )
                await queue.bind(exchange)
                queues[data_type] = queue

            # Start consumers for ALL data types lacking one — not just those
            # with a current backlog. A type whose queue was empty at the
            # passive-declare instant still needs a consumer; otherwise messages
            # that arrive later are never consumed, because once active_connection
            # is set and consumer_tags is non-empty both periodic recovery routes
            # are permanently gated off, silently starving that data type.
            pending_counts = dict(queues_with_messages)
            for data_type in MUSICBRAINZ_DATA_TYPES:
                if data_type in queues and data_type not in consumer_tags:
                    handler = make_data_handler(data_type)
                    consumer_tag = await queues[data_type].consume(handler)
                    consumer_tags[data_type] = consumer_tag
                    _record_consumer_delta(1)
                    # Only un-complete a type that actually has a backlog, so
                    # genuinely-finished types stay marked complete.
                    if data_type in pending_counts:
                        completed_files.discard(data_type)
                    last_message_time[data_type] = time.time()
                    logger.info(
                        f"✅ Started consumer for {data_type}",
                        data_type=data_type,
                        pending_messages=pending_counts.get(data_type, 0),
                    )

            logger.info(
                "✅ Recovery complete - consumers restarted",
                active_consumers=list(consumer_tags.keys()),
            )
            idle_mode = False
        else:
            logger.info("⏳ No messages in any queue, connection remains closed")
            await temp_channel.close()
            await temp_connection.close()

    except Exception as e:
        logger.error("❌ Error during consumer recovery", error=str(e))
        try:
            await temp_channel.close()
            await temp_connection.close()
        except Exception as close_error:
            logger.warning(
                "⚠️ Error closing temporary connection after recovery failure",
                error=str(close_error),
            )
        active_connection = None
        active_channel = None
        queues = {}
        # Clear stale consumer tags: any consumers registered before the error
        # died with the now-closed connection. Leaving them behind would keep
        # len(consumer_tags) > 0 forever, permanently gating off both recovery
        # routes (stuck-check requires 0 tags) while health still reads healthy.
        _record_consumer_delta(-len(consumer_tags))
        consumer_tags.clear()


async def _insert_relationships(conn: Any, source_mbid: str, source_type: str, rels: list[dict[str, Any]]) -> None:
    """Preserve the established relationship-batch entry point."""
    await _record_processor.insert_relationships(conn, source_mbid, source_type, rels)


async def _insert_external_links(conn: Any, mbid: str, entity_type: str, links: list[dict[str, Any]]) -> None:
    """Preserve the established external-link batch entry point."""
    await _record_processor.insert_external_links(conn, mbid, entity_type, links)


async def process_artist(conn: Any, record: dict[str, Any]) -> None:
    """Insert or update a MusicBrainz artist record in PostgreSQL."""
    await _record_processor.process_artist(conn, record)


async def process_label(conn: Any, record: dict[str, Any]) -> None:
    """Insert or update a MusicBrainz label record in PostgreSQL."""
    await _record_processor.process_label(conn, record)


async def process_release(conn: Any, record: dict[str, Any]) -> None:
    """Insert or update a MusicBrainz release record in PostgreSQL."""
    await _record_processor.process_release(conn, record)


async def process_release_group(conn: Any, record: dict[str, Any]) -> None:
    """Insert or update a MusicBrainz release-group record in PostgreSQL."""
    await _record_processor.process_release_group(conn, record)


PROCESSORS: dict[str, Any] = {
    "artists": process_artist,
    "labels": process_label,
    "release-groups": process_release_group,
    "releases": process_release,
}


def make_data_handler(
    data_type: str,
) -> Any:
    """Create a per-data-type message handler that injects data_type context."""

    async def handler(message: AbstractIncomingMessage) -> DeliveryResult:
        return await on_data_message(message, data_type)

    return handler


async def _reconcile_deleted_child_rows(data_type: str, data: dict[str, Any]) -> bool:
    """Reconcile the child-row tables once every entity type has signalled completion.

    ``musicbrainz.relationships`` and ``musicbrainz.external_links`` are written by all
    four entity kinds, so the purge cannot fire on the first ``extraction_complete`` —
    it would delete every row the other three kinds have not sent yet. The boundary and
    the per-type record counts both come from the message, never from this process, so a
    restart cannot move them; see the ``_reconciliation`` module docstring. Returns
    ``False`` only when the reconciliation was attempted and failed, which is the one
    case the signal must be requeued for.
    """
    purge = stale_row_purge
    if purge is None:
        return True

    purge.record_completion(data_type, data)
    if not purge.is_latched():
        logger.info(
            "⏳ Deferring MusicBrainz delete-reconciliation until every entity type completes",
            signalled=sorted(purge.signalled),
            pending=purge.pending_data_types(),
            boundary=purge.boundary.isoformat() if purge.boundary else None,
        )
        return True

    try:
        await purge.purge()
    except Exception as error:
        logger.error(
            "❌ Delete-reconciliation failed, requeueing extraction_complete",
            error=str(error),
        )
        return False
    return True


def _reject(data_type: str, error_type: str) -> DeliveryResult:
    """Reject a delivery to the dead-letter queue, vetoing the reconciliation first.

    The veto is recorded here rather than only after ``run_delivery`` returns because
    settlement and the mark are otherwise two steps with a gap between them: with a
    prefetch above one, an ``extraction_complete`` delivery running concurrently can
    latch and purge in that gap, after this record was nacked and before the mark that
    protects it exists. Marking before the broker is told closes the window for every
    rejection this function names. ``on_data_message`` still marks afterwards, because
    the shared classifier can turn a raised failure into a rejection this function never
    sees; the mark is a set membership, so recording it twice costs nothing.

    A mark can only ever *skip* a purge, never widen one, so recording one early is the
    safe direction to be wrong in.
    """
    if stale_row_purge is not None:
        stale_row_purge.record_dead_letter(data_type)
    return DeliveryResult(Settlement.REJECT, "failed", error_type)


async def _handle_data_message(message: AbstractIncomingMessage, data_type: str) -> DeliveryResult:
    """Run one local transaction and return settlement intent without touching the broker."""
    try:
        data: dict[str, Any] = loads(message.body)
    except Exception as error:
        logger.error("❌ Failed to parse message", error=str(error))
        return _reject(data_type, type(error).__name__)

    if data.get("type") == "file_complete":
        total_processed = data.get("total_processed", 0)
        logger.info(f"✅ File processing complete for {data_type}! Total records processed: {total_processed}")
        if CONSUMER_CANCEL_DELAY > 0 and data_type in queues:
            await schedule_consumer_cancellation(data_type, queues[data_type])
        completed_files.add(data_type)
        return DeliveryResult(Settlement.ACK, "skipped")

    if data.get("type") == "extraction_complete":
        logger.info(
            "🏁 Received extraction_complete signal",
            data_type=data_type,
            version=data.get("version"),
        )
        completed_files.add(data_type)
        if not await _reconcile_deleted_child_rows(data_type, data):
            # The reconciliation is this run's only chance to remove rows dropped
            # upstream, so a failure requeues the signal rather than acking past it.
            # Completion is withdrawn with it: the signal is still pending, and idle
            # detection must not treat this type as finished while it is.
            completed_files.discard(data_type)
            return DeliveryResult(Settlement.REQUEUE, "failed", "ReconciliationFailed")
        if CONSUMER_CANCEL_DELAY > 0 and data_type in queues:
            await schedule_consumer_cancellation(data_type, queues[data_type])
        return DeliveryResult(Settlement.ACK, "skipped")

    if "id" not in data:
        logger.error("❌ Message missing 'id' field", data=data)
        return _reject(data_type, "ValidationError")

    data_id: str = data["id"]
    if not data_id:
        logger.warning("⚠️ Nacking record with empty mbid/id", data_type=data_type)
        return _reject(data_type, "ValidationError")

    try:
        uuid.UUID(data_id)
    except ValueError, AttributeError, TypeError:
        logger.warning(
            "⚠️ Nacking record with non-UUID mbid/id",
            data_type=data_type,
            data_id=data_id,
        )
        return _reject(data_type, "ValidationError")

    processor = PROCESSORS.get(data_type)
    if processor is None:
        logger.error("❌ No processor for data type", data_type=data_type)
        return _reject(data_type, "UnknownEntity")

    record_name = data.get("name", "Unknown")
    logger.debug(
        "🔄 Processing record",
        data_type=data_type[:-1],
        data_id=data_id,
        record_name=record_name,
    )

    if connection_pool is None:
        raise RuntimeError("Connection pool not initialized")

    async with connection_pool.connection() as conn:
        await conn.set_autocommit(False)
        async with conn.transaction():
            await processor(conn, data)

        logger.debug(
            "🐘 Updated record in PostgreSQL",
            data_type=data_type[:-1],
            data_id=data_id,
        )

    return DeliveryResult(Settlement.ACK, "processed")


async def on_data_message(message: AbstractIncomingMessage, data_type: str) -> DeliveryResult:
    """Process and settle one delivery through the shared settlement authority."""
    if shutdown_requested:
        # A still-subscribed consumer would immediately redeliver a requeue and burn the
        # quorum delivery budget. Connection close requeues this unsettled delivery once.
        logger.debug("🛑 Shutdown requested, leaving message unacked for redelivery")
        return DeliveryResult(Settlement.DEFER, "deferred")

    entity = _ENTITY_LABELS.get(data_type, data_type)
    try:
        destination = catalog_queue_name(AMQP_CONSUMER_ID, data_type)
    except ValueError:
        destination = "unknown"

    classifier = _MusicBrainzFailureClassifier(data_type)
    result = await run_delivery(
        message,
        lambda: _handle_data_message(message, data_type),
        classifier=classifier,
        observer=_delivery_observer,
        destination=destination,
        entity=entity,
        headers=message.headers,
        wait_before_requeue=classifier.wait_before_requeue,
    )
    if result.settlement is Settlement.REJECT and stale_row_purge is not None:
        # A rejected delivery is dead-lettered with its row never upserted, so that
        # row's updated_at was never refreshed even though the record is still present
        # upstream. Purging on top of that would delete a still-current record beyond
        # the dead-letter queue, so any rejection this run vetoes the reconciliation.
        stale_row_purge.record_dead_letter(data_type)
    if result.settlement is Settlement.ACK and result.outcome == "processed":
        outage_backoff.reset()
        if data_type in message_counts:
            message_counts[data_type] += 1
            last_message_time[data_type] = time.time()
            if message_counts[data_type] % progress_interval == 0:
                logger.info(
                    "📊 Processed records in PostgreSQL",
                    count=message_counts[data_type],
                    data_type=data_type,
                )
    return result


async def progress_reporter() -> None:
    """Report processing progress periodically."""
    global idle_mode

    report_count = 0
    startup_time = time.time()
    last_idle_log = 0.0

    while not shutdown_requested:
        if report_count < 3:
            await asyncio.sleep(10)
        else:
            await asyncio.sleep(30)
        report_count += 1

        if len(completed_files) == len(MUSICBRAINZ_DATA_TYPES):
            continue

        total = sum(message_counts.values())
        current_time = time.time()

        if not idle_mode and total == 0 and (current_time - startup_time) >= STARTUP_IDLE_TIMEOUT:
            idle_mode = True
            last_idle_log = current_time
            logger.info(
                f"😴 No messages received after {STARTUP_IDLE_TIMEOUT}s, entering idle mode. Consumers remain connected, reporting paused.",
                startup_idle_timeout=STARTUP_IDLE_TIMEOUT,
            )
            continue

        if idle_mode:
            if total > 0:
                idle_mode = False
                logger.info("🔄 Messages detected, resuming normal operation")
            elif (current_time - last_idle_log) >= IDLE_LOG_INTERVAL:
                last_idle_log = current_time
                logger.info(
                    "😴 Idle mode - waiting for messages. Consumers connected.",
                )
            continue

        stalled_consumers = []
        for data_type, last_time in last_message_time.items():
            if data_type not in completed_files and last_time > 0 and (current_time - last_time) > 120:
                stalled_consumers.append(data_type)

        if stalled_consumers:
            logger.error(f"⚠️ Stalled consumers detected: {stalled_consumers}. No messages processed for >2 minutes.")

        progress_parts = []
        for data_type in ["artists", "labels", "release-groups", "releases"]:
            emoji = "✅ " if data_type in completed_files else ""
            progress_parts.append(f"{emoji}{data_type.capitalize()}: {message_counts[data_type]}")

        logger.info(f"📊 MusicBrainz PostgreSQL Progress: {total} total messages processed ({', '.join(progress_parts)})")

        if total == 0:
            logger.info("⏳ Waiting for messages to process...")
        elif all(current_time - last_time < 5 for last_time in last_message_time.values() if last_time > 0):
            logger.info("✅ All consumers actively processing")
        elif any(last_time > 0 and 5 < current_time - last_time < 120 for last_time in last_message_time.values()):
            slow_consumers = [dt for dt, lt in last_message_time.items() if lt > 0 and 5 < current_time - lt < 120]
            logger.warning(
                f"⚠️ Slow consumers detected: {slow_consumers}",
                slow_consumers=slow_consumers,
            )

        active_consumers = list(consumer_tags.keys())
        canceled_consumers = [dt for dt in MUSICBRAINZ_DATA_TYPES if dt not in consumer_tags and dt in completed_files]

        if canceled_consumers:
            logger.info(
                f"🔧 Canceled consumers: {canceled_consumers}",
                canceled_consumers=canceled_consumers,
            )
        if active_consumers:
            logger.info(
                f"✅ Active consumers: {active_consumers}",
                active_consumers=active_consumers,
            )


async def main() -> None:
    """Main entry point for the MusicBrainz SQL loader service."""
    global connection_pool, config, connection_params, queues, rabbitmq_manager, active_connection, active_channel, connection_check_task
    global stale_row_purge

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setup_logging(SERVICE_NAME, log_file=Path(f"/logs/{SERVICE_NAME}.log"))
    setup_telemetry(AMQP_CONSUMER_ID)
    # Sample this loop's scheduling delay into groovemap.runtime.event_loop.lag. It has to be
    # started from the running loop, which is why it lives here and not next to the process
    # metrics setup_telemetry installs. shutdown_telemetry() cancels the sampling task.
    start_event_loop_monitor()
    logger.info("🚀 Starting GrooveMap musicbrainz-sql-loader with connection pooling")

    startup_delay = int(os.environ.get("STARTUP_DELAY", "5"))
    if startup_delay > 0:
        logger.info(
            f"⏳ Waiting {startup_delay} seconds for dependent services to start...",
            startup_delay=startup_delay,
        )
        await asyncio.sleep(startup_delay)

    health_server = HealthServer(8010, get_health_data)
    health_server.start_background()
    logger.info("🏥 Health server started on port 8010")

    try:
        config = MusicBrainzSQLLoaderConfig.from_env()
    except ValueError as e:
        logger.error("❌ Configuration error", error=str(e))
        return

    host, port = parse_postgres_host_port(config.postgres_host)

    connection_params = {
        "host": str(host),
        "port": int(port),
        "dbname": str(config.postgres_database),
        "user": str(config.postgres_username),
        "password": str(config.postgres_password),
    }

    try:
        connection_pool = AsyncPostgreSQLPool(
            connection_params=connection_params,
            max_connections=config.postgres_pool_max_size,
            min_connections=config.postgres_pool_min_size,
            max_retries=5,
            health_check_interval=30,
        )
        await connection_pool.initialize()
        logger.info("🐘 Connected to PostgreSQL with async resilient connection pool")
        logger.info(
            "✅ Async connection pool initialized (min: %d, max: %d connections)",
            config.postgres_pool_min_size,
            config.postgres_pool_max_size,
        )
    except Exception as e:
        logger.error("❌ Failed to initialize connection pool", error=str(e))
        return

    # Delete-reconciliation is maintenance, not ingestion, and this loader issues no DDL:
    # it probes for the column groovemap-database-schema owns and stays off until it is
    # there. Both the probe answering False and the probe itself failing leave
    # `stale_row_purge` None, so every call site no-ops, the upserts emit no reference to
    # a column the schema has not declared, and the loader keeps loading either way.
    stale_row_purge = None
    try:
        if await reconciliation_columns_present(connection_pool, logger):
            _persistence_writer.set_refresh_updated_at(True)
            stale_row_purge = StaleChildRowPurge(connection_pool, logger)
    except Exception as e:
        logger.error("❌ Delete-reconciliation unavailable this run", error=str(e))

    print(STARTUP_BANNER)

    rabbitmq_manager = AsyncResilientRabbitMQ(
        connection_url=config.amqp_connection,
        max_retries=10,
        heartbeat=600,
        connection_attempts=10,
        retry_delay=5.0,
    )

    max_startup_retries = 5
    startup_retry = 0
    amqp_connection = None

    while startup_retry < max_startup_retries and not shutdown_requested:
        try:
            logger.info(
                "🐰 Attempting to connect to RabbitMQ",
                attempt=startup_retry + 1,
                max_attempts=max_startup_retries,
            )
            amqp_connection = await rabbitmq_manager.connect()
            active_connection = amqp_connection
            break
        except Exception as e:
            startup_retry += 1
            if startup_retry < max_startup_retries:
                wait_time = min(30, 5 * startup_retry)
                logger.warning(
                    "⚠️ RabbitMQ connection failed. Retrying...",
                    error=str(e),
                    wait_seconds=wait_time,
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(
                    "❌ Failed to connect to AMQP broker",
                    max_attempts=max_startup_retries,
                    error=str(e),
                )
                return

    if amqp_connection is None:
        logger.error("❌ No AMQP connection available")
        return

    async with amqp_connection:
        channel = await amqp_connection.channel()
        active_channel = channel

        # Channel-global QoS bounds total in-flight handlers across all consumers to the
        # pool capacity, so the connection pool is never oversubscribed (see _channel_prefetch).
        prefetch = _channel_prefetch()
        await channel.set_qos(prefetch_count=prefetch, global_=True)
        logger.info(
            "🔧 QoS prefetch configured (channel-global, coupled to pool max)",
            prefetch_count=prefetch,
        )

        queues = {}
        for data_type in MUSICBRAINZ_DATA_TYPES:
            exchange_name = catalog_exchange_name(data_type)
            queue_name = catalog_queue_name(AMQP_CONSUMER_ID, data_type)
            dlx_name = catalog_dead_letter_exchange_name(AMQP_CONSUMER_ID, data_type)
            dlq_name = catalog_dead_letter_queue_name(AMQP_CONSUMER_ID, data_type)

            exchange = await channel.declare_exchange(exchange_name, AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)

            dlx_exchange = await channel.declare_exchange(dlx_name, AMQP_EXCHANGE_TYPE, durable=True, auto_delete=False)

            dlq = await channel.declare_queue(
                auto_delete=False,
                durable=True,
                name=dlq_name,
                arguments={"x-queue-type": "classic"},
            )
            await dlq.bind(dlx_exchange)

            queue_args = {
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": dlx_name,
                "x-delivery-limit": 20,
            }
            queue = await channel.declare_queue(
                auto_delete=False,
                durable=True,
                name=queue_name,
                arguments=queue_args,
            )
            await queue.bind(exchange)
            queues[data_type] = queue

        for data_type in MUSICBRAINZ_DATA_TYPES:
            handler = make_data_handler(data_type)
            consumer_tags[data_type] = await queues[data_type].consume(handler)
            _record_consumer_delta(1)

        logger.info(
            f"🚀 {SERVICE_NAME} started! Connected to AMQP broker ({len(MUSICBRAINZ_DATA_TYPES)} fanout exchanges). "
            f"Consuming from {len(MUSICBRAINZ_DATA_TYPES)} queues with connection pool (max {config.postgres_pool_max_size} connections). "
            "Ready to process MusicBrainz messages into PostgreSQL. Press CTRL+C to exit"
        )

        progress_task = asyncio.create_task(progress_reporter())

        connection_check_task = asyncio.create_task(periodic_queue_checker())
        logger.info(
            f"🔄 Started periodic queue checker (interval: {QUEUE_CHECK_INTERVAL}s)",
            QUEUE_CHECK_INTERVAL=QUEUE_CHECK_INTERVAL,
        )

        try:
            shutdown_event = asyncio.Event()

            while not shutdown_requested:
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=1.0)
                    break
                except TimeoutError:
                    continue

        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, shutting down gracefully")
        finally:
            # Stop new deliveries FIRST, before the multi-second flush/teardown
            # below: a still-subscribed consumer keeps being handed messages it
            # can only leave unacked.
            await cancel_all_consumers()

            progress_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await progress_task

            if connection_check_task:
                connection_check_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await connection_check_task
                logger.info("✅ Queue checker task stopped")

            for task in list(consumer_cancel_tasks.values()):
                task.cancel()

            await close_rabbitmq_connection()

            try:
                if connection_pool:
                    await connection_pool.close()
                    logger.info("✅ Async connection pool closed")
            except Exception as e:
                logger.warning("⚠️ Error closing connection pool", error=str(e))

        health_server.stop()

        shutdown_telemetry()


def cli() -> None:
    """Run the async service from a console-script entry point."""
    run(main())


if __name__ == "__main__":
    try:
        cli()
    except KeyboardInterrupt:
        logger.warning("⚠️ Application interrupted")
    except Exception as e:
        logger.error("❌ Application error", error=str(e))
    finally:
        logger.info(f"✅ {SERVICE_NAME} service shutdown complete")
