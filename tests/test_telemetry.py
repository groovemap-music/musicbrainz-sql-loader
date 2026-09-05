"""Tests for the OpenTelemetry domain metrics and spans this service records.

Every assertion here is about the shape dashboards and traces depend on: instrument name,
unit, span name, span kind, and the closed attribute set from the OTEL program conventions.
In-memory providers for both signals, installed as the active providers, make these fully
local — no collector, no network.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from common import AsyncPostgreSQLPool, get_tracer, runtime_metrics, telemetry
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
from psycopg.errors import IntegrityError

import brainztableinator.brainztableinator as bt
from brainztableinator.brainztableinator import (
    _insert_external_links,
    _insert_relationships,
    cancel_all_consumers,
    on_data_message,
)


if TYPE_CHECKING:
    from collections.abc import Iterator

    from opentelemetry.sdk.metrics.export import Metric
    from opentelemetry.sdk.trace import ReadableSpan


# A known W3C trace context, as an extractor's publish would leave it on an AMQP message.
UPSTREAM_TRACE_ID = 0x4BF92F3577B34DA6A3CE929D0E0E4736
UPSTREAM_SPAN_ID = 0x00F067AA0BA902B7
UPSTREAM_TRACEPARENT = f"00-{UPSTREAM_TRACE_ID:032x}-{UPSTREAM_SPAN_ID:016x}-01"

ARTISTS_QUEUE = "groovemap-musicbrainz-brainztableinator-artists"


class Collector:
    """In-memory providers for both signals, plus helpers for reading what was recorded."""

    def __init__(self) -> None:
        self.reader = InMemoryMetricReader()
        self.provider = SdkMeterProvider(metric_readers=[self.reader])
        self.span_exporter = InMemorySpanExporter()
        self.tracer_provider = SdkTracerProvider()
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.span_exporter))

    def metrics(self) -> dict[str, Metric]:
        """Collect once and return every recorded metric by name."""
        data = self.reader.get_metrics_data()
        if data is None:
            return {}
        return {
            metric.name: metric
            for resource_metrics in data.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
        }

    def points(self, name: str) -> list[Any]:
        """Return the data points recorded for one metric name."""
        metric = self.metrics().get(name)
        return [] if metric is None else list(metric.data.data_points)

    def attributes(self, name: str) -> list[dict[str, Any]]:
        """Return the attribute dicts recorded for one metric name."""
        return [dict(point.attributes) for point in self.points(name)]

    def point_for(self, name: str, **attrs: Any) -> Any:
        """Return the single data point matching every given attribute."""
        matching = [point for point in self.points(name) if all(dict(point.attributes).get(k) == v for k, v in attrs.items())]
        assert len(matching) == 1, f"expected exactly one {name} point matching {attrs!r}, got {len(matching)}"
        return matching[0]

    def spans(self, kind: SpanKind | None = None) -> list[ReadableSpan]:
        """Return every finished span, optionally narrowed to one kind."""
        finished = self.span_exporter.get_finished_spans()
        return [span for span in finished if kind is None or span.kind is kind]

    def span_named(self, name: str) -> ReadableSpan:
        """Return the single finished span with this name."""
        matching = [span for span in self.spans() if span.name == name]
        assert len(matching) == 1, f"expected exactly one {name!r} span, got {[span.name for span in self.spans()]}"
        return matching[0]


@pytest.fixture
def collector(monkeypatch: pytest.MonkeyPatch) -> Iterator[Collector]:
    """Install an in-memory provider and make every instrument cache rebuild against it.

    Two caches read the installed provider: this module's own (rebuilt unconditionally by
    ``reset_metric_instruments``) and ``common.runtime_metrics``'s (rebuilt only when
    ``telemetry.provider_generation()`` changes, hence bumping ``_generation`` here too).
    """
    active = Collector()
    monkeypatch.setattr(telemetry, "_provider", active.provider)
    monkeypatch.setattr(telemetry, "_tracer_provider", active.tracer_provider)
    monkeypatch.setattr(telemetry, "_generation", telemetry.provider_generation() + 1)
    runtime_metrics.reset_instruments()
    bt.reset_metric_instruments()
    assert telemetry.tracer_provider() is active.tracer_provider
    yield active
    monkeypatch.setattr(telemetry, "_provider", None)
    monkeypatch.setattr(telemetry, "_tracer_provider", None)
    runtime_metrics.reset_instruments()
    bt.reset_metric_instruments()


def _valid_message(body: bytes, headers: dict[str, Any] | None = None) -> AsyncMock:
    mock_message = AsyncMock()
    mock_message.body = body
    # aio-pika hands back the delivery's header table, or None when the publisher sent none.
    mock_message.headers = headers
    return mock_message


def _connected_pool(mock_conn: AsyncMock) -> MagicMock:
    """A mock AsyncPostgreSQLPool whose connection() context manager yields mock_conn."""
    mock_tx_cm = AsyncMock()
    mock_tx_cm.__aenter__ = AsyncMock(return_value=None)
    mock_tx_cm.__aexit__ = AsyncMock(return_value=None)
    mock_conn.transaction = MagicMock(return_value=mock_tx_cm)

    mock_conn_cm = AsyncMock()
    mock_conn_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn_cm.__aexit__ = AsyncMock(return_value=None)

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn_cm)
    return mock_pool


# ===========================================================================
# groovemap.pipeline.messages / .message.duration
# ===========================================================================


class TestPipelineMessageMetrics:
    """on_data_message records groovemap.pipeline.messages / .message.duration."""

    @pytest.mark.asyncio
    async def test_successful_processing_records_processed_outcome(self, collector: Collector) -> None:
        mock_message = _valid_message(b'{"id": "550e8400-e29b-41d4-a716-446655440000", "name": "Test Artist"}')
        mock_conn = AsyncMock()
        mock_pool = _connected_pool(mock_conn)
        mock_processor = AsyncMock()

        with (
            patch("brainztableinator.brainztableinator.shutdown_requested", False),
            patch("brainztableinator.brainztableinator.completed_files", set()),
            patch("brainztableinator.brainztableinator.connection_pool", mock_pool),
            patch(
                "brainztableinator.brainztableinator.message_counts",
                {"artists": 0, "labels": 0, "release-groups": 0, "releases": 0},
            ),
            patch(
                "brainztableinator.brainztableinator.last_message_time",
                {"artists": 0.0, "labels": 0.0, "release-groups": 0.0, "releases": 0.0},
            ),
            patch.dict("brainztableinator.brainztableinator.PROCESSORS", {"artists": mock_processor}),
        ):
            await on_data_message(mock_message, "artists")

        point = collector.point_for(bt.PIPELINE_MESSAGES, source="musicbrainz", entity="artist", outcome="processed")
        assert point.value == 1

        duration_point = collector.point_for(bt.PIPELINE_MESSAGE_DURATION, source="musicbrainz", entity="artist")
        assert duration_point.sum >= 0

        consumed_point = collector.point_for(
            bt.MESSAGING_CONSUMED_MESSAGES,
            **{
                "messaging.system": "rabbitmq",
                "messaging.destination.name": "groovemap-musicbrainz-brainztableinator-artists",
                "messaging.operation.name": "process",
            },
        )
        assert consumed_point.value == 1
        assert "error.type" not in collector.attributes(bt.MESSAGING_CONSUMED_MESSAGES)[0]

    @pytest.mark.asyncio
    async def test_file_complete_records_skipped_outcome(self, collector: Collector) -> None:
        mock_message = _valid_message(b'{"type": "file_complete", "total_processed": 100}')

        with (
            patch("brainztableinator.brainztableinator.shutdown_requested", False),
            patch("brainztableinator.brainztableinator.completed_files", set()),
            patch("brainztableinator.brainztableinator.CONSUMER_CANCEL_DELAY", 0),
            patch("brainztableinator.brainztableinator.connection_pool", MagicMock()),
        ):
            await on_data_message(mock_message, "release-groups")

        point = collector.point_for(bt.PIPELINE_MESSAGES, source="musicbrainz", entity="release-group", outcome="skipped")
        assert point.value == 1

    @pytest.mark.asyncio
    async def test_missing_id_records_failed_outcome_and_error_type(self, collector: Collector) -> None:
        mock_message = _valid_message(b'{"name": "No id here"}')

        with (
            patch("brainztableinator.brainztableinator.shutdown_requested", False),
            patch("brainztableinator.brainztableinator.connection_pool", MagicMock()),
        ):
            await on_data_message(mock_message, "labels")

        point = collector.point_for(bt.PIPELINE_MESSAGES, source="musicbrainz", entity="label", outcome="failed")
        assert point.value == 1

        consumed_attrs = collector.attributes(bt.MESSAGING_CONSUMED_MESSAGES)
        assert any(attrs.get("error.type") == "ValidationError" for attrs in consumed_attrs)

    @pytest.mark.asyncio
    async def test_data_error_records_failed_outcome_with_exception_class_name(self, collector: Collector) -> None:
        mock_message = _valid_message(b'{"id": "550e8400-e29b-41d4-a716-446655440000", "name": "Test"}')
        mock_conn = AsyncMock()
        mock_pool = _connected_pool(mock_conn)
        mock_processor = AsyncMock(side_effect=IntegrityError("boom"))

        with (
            patch("brainztableinator.brainztableinator.shutdown_requested", False),
            patch("brainztableinator.brainztableinator.connection_pool", mock_pool),
            patch.dict("brainztableinator.brainztableinator.PROCESSORS", {"releases": mock_processor}),
        ):
            await on_data_message(mock_message, "releases")

        point = collector.point_for(bt.PIPELINE_MESSAGES, source="musicbrainz", entity="release", outcome="failed")
        assert point.value == 1

        consumed_attrs = collector.attributes(bt.MESSAGING_CONSUMED_MESSAGES)
        assert any(attrs.get("error.type") == "IntegrityError" for attrs in consumed_attrs)

    @pytest.mark.asyncio
    async def test_shutdown_requested_records_nothing(self, collector: Collector) -> None:
        mock_message = _valid_message(b'{"id": "550e8400-e29b-41d4-a716-446655440000"}')

        with patch("brainztableinator.brainztableinator.shutdown_requested", True):
            await on_data_message(mock_message, "artists")

        assert collector.points(bt.PIPELINE_MESSAGES) == []
        assert collector.points(bt.MESSAGING_CONSUMED_MESSAGES) == []


# ===========================================================================
# groovemap.pipeline.batch.size / .batch.flush.duration
# ===========================================================================


class TestBatchFlushMetrics:
    """_insert_relationships / _insert_external_links record batch flush metrics."""

    @pytest.mark.asyncio
    async def test_relationships_batch_records_processed_outcome(self, collector: Collector) -> None:
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor_cm = AsyncMock()
        mock_cursor_cm.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor_cm.__aexit__ = AsyncMock(return_value=None)
        mock_conn.cursor = MagicMock(return_value=mock_cursor_cm)

        rels = [{"target_mbid": "550e8400-e29b-41d4-a716-446655440001", "target_type": "label", "type": "member of band"}]
        await _insert_relationships(mock_conn, "550e8400-e29b-41d4-a716-446655440000", "artist", rels)

        point = collector.point_for(bt.PIPELINE_BATCH_SIZE, store="postgresql", entity="artist", outcome="processed")
        assert point.sum == 1

        duration_point = collector.point_for(bt.PIPELINE_BATCH_FLUSH_DURATION, store="postgresql", entity="artist", outcome="processed")
        assert duration_point.count == 1

    @pytest.mark.asyncio
    async def test_relationships_batch_records_skipped_outcome_when_empty(self, collector: Collector) -> None:
        mock_conn = AsyncMock()

        await _insert_relationships(mock_conn, "550e8400-e29b-41d4-a716-446655440000", "label", [])

        point = collector.point_for(bt.PIPELINE_BATCH_SIZE, store="postgresql", entity="label", outcome="skipped")
        assert point.sum == 0
        mock_conn.cursor.assert_not_called()

    @pytest.mark.asyncio
    async def test_relationships_batch_records_failed_outcome_and_reraises(self, collector: Collector) -> None:
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor.executemany = AsyncMock(side_effect=IntegrityError("boom"))
        mock_cursor_cm = AsyncMock()
        mock_cursor_cm.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor_cm.__aexit__ = AsyncMock(return_value=None)
        mock_conn.cursor = MagicMock(return_value=mock_cursor_cm)

        rels = [{"target_mbid": "550e8400-e29b-41d4-a716-446655440001", "target_type": "release", "type": "performer"}]

        with pytest.raises(IntegrityError):
            await _insert_relationships(mock_conn, "550e8400-e29b-41d4-a716-446655440000", "release-group", rels)

        point = collector.point_for(bt.PIPELINE_BATCH_SIZE, store="postgresql", entity="release-group", outcome="failed")
        assert point.sum == 1

    @pytest.mark.asyncio
    async def test_external_links_batch_records_processed_outcome(self, collector: Collector) -> None:
        mock_conn = AsyncMock()
        mock_cursor = AsyncMock()
        mock_cursor_cm = AsyncMock()
        mock_cursor_cm.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cursor_cm.__aexit__ = AsyncMock(return_value=None)
        mock_conn.cursor = MagicMock(return_value=mock_cursor_cm)

        links = [{"url": "https://example.com", "service": "bandcamp"}]
        await _insert_external_links(mock_conn, "550e8400-e29b-41d4-a716-446655440000", "release", links)

        point = collector.point_for(bt.PIPELINE_BATCH_SIZE, store="postgresql", entity="release", outcome="processed")
        assert point.sum == 1


# ===========================================================================
# groovemap.pipeline.consumers.active
# ===========================================================================


class TestConsumersActiveGauge:
    """Consumer start/stop paths adjust groovemap.pipeline.consumers.active."""

    @pytest.mark.asyncio
    async def test_cancel_all_consumers_decrements_the_gauge(self, collector: Collector) -> None:
        mock_queue = AsyncMock()

        with (
            patch("brainztableinator.brainztableinator.consumer_tags", {"artists": "ctag-1"}),
            patch("brainztableinator.brainztableinator.queues", {"artists": mock_queue}),
        ):
            await cancel_all_consumers()

        point = collector.point_for(bt.PIPELINE_CONSUMERS_ACTIVE, source="musicbrainz")
        assert point.value == -1

    def test_record_consumer_delta_adds_to_the_gauge(self, collector: Collector) -> None:
        bt._record_consumer_delta(1)
        bt._record_consumer_delta(1)
        bt._record_consumer_delta(-1)

        point = collector.point_for(bt.PIPELINE_CONSUMERS_ACTIVE, source="musicbrainz")
        assert point.value == 1


# ===========================================================================
# db.client.operation.duration (shared wrapper — verify it fires on this
# service's code path: connection_pool.connection(), the same call site
# on_data_message uses)
# ===========================================================================


class _FakeConnection:
    """Minimal stand-in for a psycopg.AsyncConnection accepted by AsyncPostgreSQLPool."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class TestDbClientMetricsFireViaSharedWrapper:
    """AsyncPostgreSQLPool.connection() — the exact call site on_data_message uses —
    already emits db.client.operation.duration via common.runtime_metrics, without any
    code in this service. This exercises that real wrapper end to end."""

    @pytest.mark.asyncio
    async def test_db_client_operation_duration_recorded_for_a_pooled_connection(self, collector: Collector) -> None:
        pool = AsyncPostgreSQLPool(
            connection_params={"host": "localhost", "port": 5432, "dbname": "test", "user": "test", "password": "test"},
            max_connections=2,
            min_connections=1,
            health_check_interval=3600,
        )

        with (
            patch.object(pool, "_create_connection", AsyncMock(return_value=_FakeConnection())),
            patch.object(pool, "_test_connection", AsyncMock(return_value=True)),
        ):
            await pool.initialize()
            try:
                async with pool.connection() as conn:
                    assert isinstance(conn, _FakeConnection)
            finally:
                await pool.close()

        point = collector.point_for(
            "db.client.operation.duration",
            **{"db.system.name": "postgresql", "db.operation.name": "session"},
        )
        assert point.count == 1
        assert "error.type" not in dict(point.attributes)


# ===========================================================================
# process {queue} — the CONSUMER span
# ===========================================================================


def _cursor_connection() -> AsyncMock:
    """A mock connection whose cursor() is an async context manager over a mock cursor."""
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor_cm = AsyncMock()
    mock_cursor_cm.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor_cm.__aexit__ = AsyncMock(return_value=None)
    mock_conn.cursor = MagicMock(return_value=mock_cursor_cm)
    return mock_conn


async def _deliver_artist(message: AsyncMock, processor: Any, conn: AsyncMock | None = None) -> None:
    """Run one artists delivery through on_data_message with a stubbed pool and processor."""
    mock_pool = _connected_pool(conn if conn is not None else AsyncMock())
    with (
        patch("brainztableinator.brainztableinator.shutdown_requested", False),
        patch("brainztableinator.brainztableinator.completed_files", set()),
        patch("brainztableinator.brainztableinator.connection_pool", mock_pool),
        patch(
            "brainztableinator.brainztableinator.message_counts",
            {"artists": 0, "labels": 0, "release-groups": 0, "releases": 0},
        ),
        patch(
            "brainztableinator.brainztableinator.last_message_time",
            {"artists": 0.0, "labels": 0.0, "release-groups": 0.0, "releases": 0.0},
        ),
        patch.dict("brainztableinator.brainztableinator.PROCESSORS", {"artists": processor}),
    ):
        await on_data_message(message, "artists")


class TestConsumerSpan:
    """on_data_message opens `process {queue}` and joins the publisher's trace."""

    @pytest.mark.asyncio
    async def test_span_joins_the_trace_carried_by_the_message_headers(self, collector: Collector) -> None:
        """The whole point of the wave-2 adoption: one trace from the extractor's publish to
        this consumer, carried by the traceparent the broker delivered."""
        message = _valid_message(
            b'{"id": "550e8400-e29b-41d4-a716-446655440000", "name": "Test Artist"}',
            headers={"traceparent": UPSTREAM_TRACEPARENT},
        )

        await _deliver_artist(message, AsyncMock())

        span = collector.span_named(f"process {ARTISTS_QUEUE}")
        assert span.kind is SpanKind.CONSUMER
        assert span.context is not None
        assert span.context.trace_id == UPSTREAM_TRACE_ID
        assert span.parent is not None
        assert span.parent.span_id == UPSTREAM_SPAN_ID
        assert dict(span.attributes or {}) == {
            "messaging.system": "rabbitmq",
            "messaging.destination.name": ARTISTS_QUEUE,
            "messaging.operation.name": "process",
            "outcome": "processed",
        }
        assert span.status.status_code is not StatusCode.ERROR

    @pytest.mark.asyncio
    async def test_bytes_traceparent_joins_the_same_trace(self, collector: Collector) -> None:
        """Some AMQP clients hand header values back as bytes; the trace must survive that."""
        message = _valid_message(
            b'{"id": "550e8400-e29b-41d4-a716-446655440000"}',
            headers={"traceparent": UPSTREAM_TRACEPARENT.encode()},
        )

        await _deliver_artist(message, AsyncMock())

        span = collector.span_named(f"process {ARTISTS_QUEUE}")
        assert span.context is not None
        assert span.context.trace_id == UPSTREAM_TRACE_ID

    @pytest.mark.asyncio
    async def test_a_message_without_headers_starts_a_new_trace(self, collector: Collector) -> None:
        message = _valid_message(b'{"id": "550e8400-e29b-41d4-a716-446655440000"}', headers=None)

        await _deliver_artist(message, AsyncMock())

        span = collector.span_named(f"process {ARTISTS_QUEUE}")
        assert span.parent is None, "a delivery with no trace context must start a root span"
        assert span.context is not None
        assert span.context.trace_id != UPSTREAM_TRACE_ID

    @pytest.mark.asyncio
    async def test_a_malformed_traceparent_starts_a_new_trace(self, collector: Collector) -> None:
        """An unreadable trace context must not fail the message that delivered it."""
        message = _valid_message(b'{"id": "550e8400-e29b-41d4-a716-446655440000"}', headers={"traceparent": "not-a-traceparent"})

        await _deliver_artist(message, AsyncMock())

        span = collector.span_named(f"process {ARTISTS_QUEUE}")
        assert span.parent is None
        assert span.context is not None
        assert span.context.trace_id != UPSTREAM_TRACE_ID

    @pytest.mark.asyncio
    async def test_a_rejected_message_fails_the_span_with_error_type_only(self, collector: Collector) -> None:
        message = _valid_message(b'{"name": "No id here"}', headers={"traceparent": UPSTREAM_TRACEPARENT})

        with (
            patch("brainztableinator.brainztableinator.shutdown_requested", False),
            patch("brainztableinator.brainztableinator.connection_pool", MagicMock()),
        ):
            await on_data_message(message, "artists")

        span = collector.span_named(f"process {ARTISTS_QUEUE}")
        assert span.status.status_code is StatusCode.ERROR
        assert (span.attributes or {})["error.type"] == "ValidationError"
        assert (span.attributes or {})["outcome"] == "failed"
        assert span.status.description is None, "a failed span carries error.type, never a message"
        assert span.events == (), "no span event may carry a payload"

    @pytest.mark.asyncio
    async def test_shutdown_requested_opens_no_span(self, collector: Collector) -> None:
        message = _valid_message(b'{"id": "550e8400-e29b-41d4-a716-446655440000"}')

        with patch("brainztableinator.brainztableinator.shutdown_requested", True):
            await on_data_message(message, "artists")

        assert collector.spans() == []


# ===========================================================================
# flush postgresql {entity} — the batch flush span
# ===========================================================================


class TestBatchFlushSpans:
    """Each batch flush runs inside `flush postgresql {entity}`, linked to its messages."""

    @pytest.mark.asyncio
    async def test_relationships_flush_opens_the_internal_span(self, collector: Collector) -> None:
        rels = [{"target_mbid": "550e8400-e29b-41d4-a716-446655440001", "target_type": "label", "type": "member of band"}]

        await _insert_relationships(_cursor_connection(), "550e8400-e29b-41d4-a716-446655440000", "artist", rels)

        span = collector.span_named("flush postgresql artist")
        assert span.kind is SpanKind.INTERNAL
        assert dict(span.attributes or {}) == {
            "db.system.name": "postgresql",
            "groovemap.entity": "artist",
            "outcome": "processed",
        }

    @pytest.mark.asyncio
    async def test_external_links_flush_opens_the_internal_span(self, collector: Collector) -> None:
        links = [{"url": "https://example.com", "service": "bandcamp"}]

        await _insert_external_links(_cursor_connection(), "550e8400-e29b-41d4-a716-446655440000", "release", links)

        span = collector.span_named("flush postgresql release")
        assert span.kind is SpanKind.INTERNAL
        assert (span.attributes or {})["outcome"] == "processed"

    @pytest.mark.asyncio
    async def test_an_empty_batch_still_reports_a_skipped_flush(self, collector: Collector) -> None:
        """Span and metric stay in step: groovemap.pipeline.batch.flush.duration is recorded on
        the empty path too, so an operator comparing span counts to metric counts sees one
        number, not two."""
        await _insert_relationships(AsyncMock(), "550e8400-e29b-41d4-a716-446655440000", "label", [])

        span = collector.span_named("flush postgresql label")
        assert (span.attributes or {})["outcome"] == "skipped"
        assert span.status.status_code is not StatusCode.ERROR

    @pytest.mark.asyncio
    async def test_a_failed_flush_sets_error_status_and_outcome(self, collector: Collector) -> None:
        mock_conn = _cursor_connection()
        mock_conn.cursor.return_value.__aenter__.return_value.executemany = AsyncMock(side_effect=IntegrityError("boom"))
        rels = [{"target_mbid": "550e8400-e29b-41d4-a716-446655440001", "target_type": "release", "type": "performer"}]

        with pytest.raises(IntegrityError):
            await _insert_relationships(mock_conn, "550e8400-e29b-41d4-a716-446655440000", "release-group", rels)

        span = collector.span_named("flush postgresql release-group")
        assert span.status.status_code is StatusCode.ERROR
        assert (span.attributes or {})["error.type"] == "IntegrityError"
        assert (span.attributes or {})["outcome"] == "failed"
        assert span.events == ()

    @pytest.mark.asyncio
    async def test_the_flush_span_links_back_to_the_message_span(self, collector: Collector) -> None:
        """A flush covers the deliveries whose rows it writes, so it links to their spans.
        One delivery drives one flush here; common.flush_span caps the list at 64."""
        mock_conn = _cursor_connection()
        rels = [{"target_mbid": "550e8400-e29b-41d4-a716-446655440001", "target_type": "label", "type": "member of band"}]

        async def processor(conn: Any, record: dict[str, Any]) -> None:
            await _insert_relationships(conn, record["id"], "artist", rels)

        message = _valid_message(
            b'{"id": "550e8400-e29b-41d4-a716-446655440000"}',
            headers={"traceparent": UPSTREAM_TRACEPARENT},
        )
        await _deliver_artist(message, processor, conn=mock_conn)

        consume_span = collector.span_named(f"process {ARTISTS_QUEUE}")
        flush = collector.span_named("flush postgresql artist")
        assert consume_span.context is not None
        assert len(flush.links) == 1
        assert flush.links[0].context.span_id == consume_span.context.span_id
        assert flush.links[0].context.trace_id == UPSTREAM_TRACE_ID
        assert flush.context is not None
        assert flush.context.trace_id == UPSTREAM_TRACE_ID, "the flush belongs to the delivery's trace"

    @pytest.mark.asyncio
    async def test_a_flush_outside_a_delivery_carries_no_links(self, collector: Collector) -> None:
        """Nothing fabricates a link when there is no message span to point at."""
        await _insert_relationships(AsyncMock(), "550e8400-e29b-41d4-a716-446655440000", "label", [])

        assert collector.span_named("flush postgresql label").links == ()


# ===========================================================================
# Span nesting: one trace per record, from the delivery to the write
# ===========================================================================


class _FlushableConnection:
    """A pooled-connection stand-in that also serves cursors to the flush helpers."""

    def __init__(self) -> None:
        self.closed = False
        self.executed = 0

    async def close(self) -> None:
        self.closed = True

    async def set_autocommit(self, value: bool) -> None:  # noqa: ARG002 - psycopg signature
        return None

    def transaction(self) -> Any:
        return _null_async_context(None)

    def cursor(self) -> Any:
        return _null_async_context(self)

    async def executemany(self, statement: str, params: list[Any]) -> None:  # noqa: ARG002 - psycopg signature
        self.executed += 1


def _null_async_context(value: Any) -> Any:
    manager = AsyncMock()
    manager.__aenter__ = AsyncMock(return_value=value)
    manager.__aexit__ = AsyncMock(return_value=None)
    return manager


class TestSpanNesting:
    """The delivery's span is the ancestor of every span the handler opens under it."""

    @pytest.mark.asyncio
    async def test_the_pooled_session_and_flush_spans_nest_under_the_delivery(self, collector: Collector) -> None:
        """`AsyncPostgreSQLPool.connection()` — the real wrapper this handler uses — opens the
        `session postgresql` CLIENT span for as long as the connection is checked out, which in
        this service is the whole record write. The batch flush therefore runs inside that span
        rather than around it, so the tree is delivery -> pooled session -> flush.
        """
        conn = _FlushableConnection()
        rels = [{"target_mbid": "550e8400-e29b-41d4-a716-446655440001", "target_type": "label", "type": "member of band"}]

        async def processor(record_conn: Any, record: dict[str, Any]) -> None:
            await _insert_relationships(record_conn, record["id"], "artist", rels)

        pool = AsyncPostgreSQLPool(
            connection_params={"host": "localhost", "port": 5432, "dbname": "test", "user": "test", "password": "test"},
            max_connections=2,
            min_connections=1,
            health_check_interval=3600,
        )
        message = _valid_message(
            b'{"id": "550e8400-e29b-41d4-a716-446655440000"}',
            headers={"traceparent": UPSTREAM_TRACEPARENT},
        )

        with (
            patch.object(pool, "_create_connection", AsyncMock(return_value=conn)),
            patch.object(pool, "_test_connection", AsyncMock(return_value=True)),
        ):
            await pool.initialize()
            try:
                with (
                    patch("brainztableinator.brainztableinator.shutdown_requested", False),
                    patch("brainztableinator.brainztableinator.completed_files", set()),
                    patch("brainztableinator.brainztableinator.connection_pool", pool),
                    patch(
                        "brainztableinator.brainztableinator.message_counts",
                        {"artists": 0, "labels": 0, "release-groups": 0, "releases": 0},
                    ),
                    patch(
                        "brainztableinator.brainztableinator.last_message_time",
                        {"artists": 0.0, "labels": 0.0, "release-groups": 0.0, "releases": 0.0},
                    ),
                    patch.dict("brainztableinator.brainztableinator.PROCESSORS", {"artists": processor}),
                ):
                    await on_data_message(message, "artists")
            finally:
                await pool.close()

        assert conn.executed == 1
        delivery = collector.span_named(f"process {ARTISTS_QUEUE}")
        session = collector.span_named("session postgresql")
        flush = collector.span_named("flush postgresql artist")

        assert delivery.context is not None
        assert session.context is not None
        assert session.parent is not None
        assert session.parent.span_id == delivery.context.span_id
        assert flush.parent is not None
        assert flush.parent.span_id == session.context.span_id
        for span in (delivery, session, flush):
            assert span.context is not None
            assert span.context.trace_id == UPSTREAM_TRACE_ID


# ===========================================================================
# Runtime metrics: the event-loop monitor
# ===========================================================================


class TestEventLoopMonitor:
    """start_event_loop_monitor() runs from the consumer's own loop, after setup_telemetry."""

    @pytest.mark.asyncio
    @patch("brainztableinator.brainztableinator.HealthServer")
    @patch.dict("os.environ", {"STARTUP_DELAY": "0"})
    async def test_main_starts_the_monitor_from_its_running_loop_after_setup(self, mock_health_server: Mock) -> None:
        mock_health_server.return_value = MagicMock()
        order: list[str] = []
        loops: list[Any] = []

        def note(name: str) -> Any:
            def record(*_args: Any, **_kwargs: Any) -> None:
                order.append(name)

            return record

        def note_monitor(*_args: Any, **_kwargs: Any) -> None:
            order.append("start_event_loop_monitor")
            loops.append(asyncio.get_running_loop())

        with (
            patch("brainztableinator.brainztableinator.setup_logging", side_effect=note("setup_logging")),
            patch("brainztableinator.brainztableinator.setup_telemetry", side_effect=note("setup_telemetry")),
            patch("brainztableinator.brainztableinator.start_event_loop_monitor", side_effect=note_monitor),
            patch.object(bt.MusicBrainzSQLLoaderConfig, "from_env", side_effect=ValueError("stop here")),
        ):
            await bt.main()

        assert order == ["setup_logging", "setup_telemetry", "start_event_loop_monitor"]
        assert loops == [asyncio.get_running_loop()], "the monitor must be started from the consumer's loop"

    @pytest.mark.asyncio
    async def test_the_library_monitor_samples_into_the_lag_histogram(self, collector: Collector, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unpatched: this is the library call the service makes, proving the process actually
        gets groovemap.runtime.event_loop.lag rather than a mock that was called."""
        # The monitor declines to sample unless metrics are really being exported, which it
        # reads from the private SDK handle rather than the API-level one the fixture installs.
        monkeypatch.setattr(telemetry, "_sdk_provider", collector.provider)
        try:
            monitor = telemetry.start_event_loop_monitor(interval_s=0.001)
            assert monitor is not None
            await asyncio.sleep(0.05)
        finally:
            telemetry._stop_event_loop_monitors()

        points = collector.points("groovemap.runtime.event_loop.lag")
        assert points, "the monitor recorded no event-loop lag"
        assert points[0].count >= 1


# ===========================================================================
# The tracing switches: no endpoint, and traces off with metrics on
# ===========================================================================


class TestTracingSwitches:
    """Tracing is env-var-only and never fails the service."""

    @pytest.mark.asyncio
    async def test_without_an_endpoint_nothing_records_and_the_handler_still_settles(self) -> None:
        """The wave-1 regression, restated for spans: with no OTEL_EXPORTER_OTLP_ENDPOINT the
        service installs no real provider, opens no recording span, and behaves as before."""
        bt.setup_telemetry("brainztableinator")
        try:
            assert telemetry._sdk_tracer_provider is None
            assert telemetry._sdk_provider is None

            probe = get_tracer(bt.METRICS_SCOPE).start_span("probe")
            try:
                assert probe.is_recording() is False
            finally:
                probe.end()

            message = _valid_message(
                b'{"id": "550e8400-e29b-41d4-a716-446655440000"}',
                headers={"traceparent": UPSTREAM_TRACEPARENT},
            )
            processor = AsyncMock()
            await _deliver_artist(message, processor)
            processor.assert_awaited_once()
            message.ack.assert_awaited_once()
        finally:
            bt.shutdown_telemetry()

    @pytest.mark.asyncio
    async def test_traces_exporter_none_keeps_metrics_flowing_and_creates_no_spans(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A deployment that wants the process view without the trace volume sets
        OTEL_TRACES_EXPORTER=none and still gets a real MeterProvider."""
        # Refused instantly rather than routed, and with a one-second export budget, so the
        # shutdown flush below never waits on a network that is not there.
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1")
        monkeypatch.setenv("OTEL_METRIC_EXPORT_INTERVAL", "600000")
        monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")

        provider = bt.setup_telemetry("brainztableinator")
        try:
            assert isinstance(provider, SdkMeterProvider), "metrics must still be exported"
            assert telemetry._sdk_tracer_provider is None, "no tracer provider may be installed"

            probe = get_tracer(bt.METRICS_SCOPE).start_span("probe")
            try:
                assert probe.is_recording() is False
            finally:
                probe.end()

            bt.reset_metric_instruments()
            message = _valid_message(
                b'{"id": "550e8400-e29b-41d4-a716-446655440000"}',
                headers={"traceparent": UPSTREAM_TRACEPARENT},
            )
            processor = AsyncMock()
            await _deliver_artist(message, processor)
            processor.assert_awaited_once()
        finally:
            bt.shutdown_telemetry()
            bt.reset_metric_instruments()
