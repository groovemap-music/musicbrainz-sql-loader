"""Tests for the OpenTelemetry domain metrics this service records.

Every assertion here is about the shape dashboards depend on: instrument name, unit, and the
closed attribute set from the OTEL-metrics program conventions. An in-memory metric reader
installed as the active provider makes these fully local — no collector, no network.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common import AsyncPostgreSQLPool, runtime_metrics, telemetry
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
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


class Collector:
    """An in-memory provider plus helpers for reading what was recorded."""

    def __init__(self) -> None:
        self.reader = InMemoryMetricReader()
        self.provider = SdkMeterProvider(metric_readers=[self.reader])

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


@pytest.fixture
def collector(monkeypatch: pytest.MonkeyPatch) -> Iterator[Collector]:
    """Install an in-memory provider and make every instrument cache rebuild against it.

    Two caches read the installed provider: this module's own (rebuilt unconditionally by
    ``reset_metric_instruments``) and ``common.runtime_metrics``'s (rebuilt only when
    ``telemetry.provider_generation()`` changes, hence bumping ``_generation`` here too).
    """
    active = Collector()
    monkeypatch.setattr(telemetry, "_provider", active.provider)
    monkeypatch.setattr(telemetry, "_generation", telemetry.provider_generation() + 1)
    runtime_metrics.reset_instruments()
    bt.reset_metric_instruments()
    yield active
    monkeypatch.setattr(telemetry, "_provider", None)
    runtime_metrics.reset_instruments()
    bt.reset_metric_instruments()


def _valid_message(body: bytes) -> AsyncMock:
    mock_message = AsyncMock()
    mock_message.body = body
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
