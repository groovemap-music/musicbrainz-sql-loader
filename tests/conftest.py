"""Test fixtures for brainztableinator tests."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


# Every standard OpenTelemetry variable that changes what the SDK records or exports, for
# both signals. The telemetry suites assert on what an in-memory provider recorded, so they
# must not inherit ambient OTEL configuration — a CI runner or a developer's shell may set
# OTEL_SDK_DISABLED or a real collector endpoint, which would otherwise make those assertions
# fail silently (an empty collection, no error) or reach out to a real endpoint. The tracing
# half matters just as much: an inherited OTEL_TRACES_SAMPLER_ARG=0 would drop every span a
# test expects, and an inherited OTEL_PROPAGATORS without tracecontext would stop a consumer
# span from ever joining the traceparent a test hands it.
OTEL_ENVIRONMENT = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TIMEOUT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_METRICS_EXEMPLAR_FILTER",
    "OTEL_METRICS_EXPORTER",
    "OTEL_METRIC_EXPORT_INTERVAL",
    "OTEL_PROPAGATORS",
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SDK_DISABLED",
    "OTEL_SERVICE_NAME",
    "OTEL_TRACES_EXPORTER",
    "OTEL_TRACES_SAMPLER",
    "OTEL_TRACES_SAMPLER_ARG",
)


@pytest.fixture(autouse=True)
def isolated_otel_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run every test against a known-empty OpenTelemetry configuration."""
    for name in OTEL_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def service_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide deterministic dummy service configuration for isolated unit tests."""
    values = {
        "POSTGRES_DATABASE": "testdb",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PASSWORD": "test-password",
        "POSTGRES_USERNAME": "test-user",
        "RABBITMQ_HOST": "localhost",
        "RABBITMQ_PASSWORD": "guest",
        "RABBITMQ_USERNAME": "guest",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


@pytest.fixture
def mock_async_pool():
    """Mock AsyncPostgreSQLPool with async context manager support.

    Returns a function that creates a mock pool with a given connection mock.
    This allows tests to configure the connection's behavior before creating the pool.

    Usage:
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_cursor.fetchone.return_value = None

        pool = mock_async_pool(mock_conn)
        with patch("brainztableinator.brainztableinator.connection_pool", pool):
            # test code
    """

    def create_pool(mock_connection: Any = None) -> MagicMock:
        """Create a mock pool that returns the given connection."""
        if mock_connection is None:
            mock_connection = MagicMock()

        mock_pool = MagicMock()

        # Create async context manager for connection
        mock_connection_cm = AsyncMock()
        mock_connection_cm.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_connection_cm.__aexit__ = AsyncMock(return_value=None)

        # For async with connection_pool.connection() pattern:
        # connection() should return the context manager directly (not a coroutine)
        mock_pool.connection = MagicMock(return_value=mock_connection_cm)
        mock_pool.close = AsyncMock()

        return mock_pool

    return create_pool
