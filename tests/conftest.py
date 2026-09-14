"""Test fixtures for brainztableinator tests."""

from unittest.mock import MagicMock, create_autospec

import pytest
from common import AsyncPostgreSQLPool
from psycopg import AsyncConnection, AsyncCursor, AsyncTransaction


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
def mock_cursor() -> MagicMock:
    """Autospecced psycopg cursor with await-faithful query methods."""
    cursor = create_autospec(AsyncCursor, instance=True, spec_set=True)
    cursor.__aenter__.return_value = cursor
    cursor.__aexit__.return_value = False
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    return cursor


@pytest.fixture
def mock_transaction() -> MagicMock:
    """Autospecced psycopg transaction context."""
    transaction = create_autospec(AsyncTransaction, instance=True, spec_set=True)
    transaction.__aenter__.return_value = transaction
    transaction.__aexit__.return_value = False
    return transaction


@pytest.fixture
def mock_connection(mock_cursor: MagicMock, mock_transaction: MagicMock) -> MagicMock:
    """Autospecced psycopg connection with synchronous context factories."""
    connection = create_autospec(AsyncConnection, instance=True, spec_set=True)
    connection.__aenter__.return_value = connection
    connection.__aexit__.return_value = False
    connection.cursor.return_value = mock_cursor
    connection.transaction.return_value = mock_transaction
    return connection


@pytest.fixture
def mock_async_pool(mock_connection: MagicMock) -> MagicMock:
    """Autospecced resilient pool whose connection context yields the psycopg double."""
    pool = create_autospec(AsyncPostgreSQLPool, instance=True, spec_set=True)
    pool.connection.return_value = mock_connection
    return pool
