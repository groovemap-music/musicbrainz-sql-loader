"""Test fixtures for brainztableinator tests."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, create_autospec
from uuid import NAMESPACE_URL, uuid5

import pytest
from common import AsyncPostgreSQLPool
from psycopg import AsyncConnection, AsyncCursor, AsyncTransaction


if TYPE_CHECKING:
    from uuid import UUID

    from common.identity import AliasRef


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


def stub_native_id(ref: AliasRef) -> UUID:
    """Return the native id the identity stub resolves ``ref`` to, deterministically."""
    return uuid5(NAMESPACE_URL, f"{ref.provider}/{ref.entity_kind}/{ref.external_id}")


@pytest.fixture(autouse=True)
def stubbed_identity_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve native ids without a database, so the default suite stays offline.

    ``resolve_aliases`` and ``attach_aliases`` run real statements on whatever connection they
    are handed, and every ``process_*`` test hands them a mock cursor that answers nothing. The
    stubs keep those tests asserting on the writes they are actually about while still giving
    each record a stable ``gm_item_id``, so a params tuple that lost the column still fails.

    The identity tests in ``tests/test_record_processing.py`` patch the same two names from the
    test body, which runs after this fixture and therefore wins; they are where the real
    resolve-then-attach ordering is asserted.
    """

    async def resolve(_conn: Any, refs: Any, **_kwargs: Any) -> dict[AliasRef, UUID]:
        return {ref: stub_native_id(ref) for ref in refs}

    async def attach(_conn: Any, mapping: Any, **_kwargs: Any) -> dict[AliasRef, UUID]:
        return dict(mapping)

    monkeypatch.setattr("brainztableinator._record_processing.resolve_aliases", resolve)
    monkeypatch.setattr("brainztableinator._record_processing.attach_aliases", attach)
