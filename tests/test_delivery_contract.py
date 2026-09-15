"""Service-level contract for shared delivery settlement."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common import DeliveryResult, FailureKind, Settlement
from psycopg.errors import DataError, IntegrityError, InterfaceError, OperationalError

import brainztableinator.brainztableinator as service


ROOT = Path(__file__).parent.parent
RUNTIME_REVISION = "e372b6a7598ae31ee6578fdff39bc920bedd7136"
VALID_BODY = b'{"id":"550e8400-e29b-41d4-a716-446655440000","name":"Artist"}'


class StrictDelivery:
    """Fail a test immediately if production attempts a second terminal call."""

    def __init__(self, body: bytes = VALID_BODY, *, terminal_error: Exception | None = None) -> None:
        self.body = body
        self.headers: dict[str, Any] = {}
        self.calls: list[tuple[str, bool | None]] = []
        self._terminal_error = terminal_error

    async def ack(self) -> None:
        self._record("ack", None)

    async def nack(self, *, requeue: bool) -> None:
        self._record("nack", requeue)

    def _record(self, operation: str, requeue: bool | None) -> None:
        if self.calls:
            raise AssertionError(f"second terminal call after {self.calls!r}")
        self.calls.append((operation, requeue))
        if self._terminal_error is not None:
            raise self._terminal_error


def _pool() -> tuple[MagicMock, AsyncMock]:
    connection = AsyncMock()
    transaction = AsyncMock()
    transaction.__aenter__ = AsyncMock(return_value=None)
    transaction.__aexit__ = AsyncMock(return_value=None)
    connection.transaction = MagicMock(return_value=transaction)
    checkout = AsyncMock()
    checkout.__aenter__ = AsyncMock(return_value=connection)
    checkout.__aexit__ = AsyncMock(return_value=None)
    pool = MagicMock()
    pool.connection = MagicMock(return_value=checkout)
    return pool, connection


@pytest.mark.asyncio
async def test_ack_uses_one_transaction_and_one_terminal_call() -> None:
    delivery = StrictDelivery()
    processor = AsyncMock()
    pool, connection = _pool()
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "connection_pool", pool),
        patch.dict(service.PROCESSORS, {"artists": processor}),
        patch.object(service, "message_counts", {"artists": 0}),
        patch.object(service, "last_message_time", {"artists": 0.0}),
    ):
        result = await service.on_data_message(delivery, "artists")  # type: ignore[arg-type]

    assert result == DeliveryResult(Settlement.ACK, "processed")
    assert delivery.calls == [("ack", None)]
    pool.connection.assert_called_once_with()
    connection.transaction.assert_called_once_with()
    processor.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [b'{"name":"missing"}', b'{"id":""}', b'{"id":"not-a-uuid"}'],
)
async def test_invalid_input_is_rejected_once(body: bytes) -> None:
    delivery = StrictDelivery(body)
    with patch.object(service, "shutdown_requested", False):
        result = await service.on_data_message(delivery, "artists")  # type: ignore[arg-type]
    assert result == DeliveryResult(Settlement.REJECT, "failed", "ValidationError")
    assert delivery.calls == [("nack", False)]


@pytest.mark.asyncio
async def test_unknown_entity_is_rejected_once() -> None:
    delivery = StrictDelivery()
    with patch.object(service, "shutdown_requested", False):
        result = await service.on_data_message(delivery, "not-an-entity")  # type: ignore[arg-type]
    assert result == DeliveryResult(Settlement.REJECT, "failed", "UnknownEntity")
    assert delivery.calls == [("nack", False)]


@pytest.mark.asyncio
async def test_database_outage_waits_then_requeues_once() -> None:
    delivery = StrictDelivery()
    pool = MagicMock()
    pool.connection.side_effect = OperationalError("offline")
    wait = AsyncMock()
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "connection_pool", pool),
        patch.object(service.outage_backoff, "wait", wait),
    ):
        result = await service.on_data_message(delivery, "artists")  # type: ignore[arg-type]
    assert result == DeliveryResult(Settlement.REQUEUE, "transient", "OperationalError")
    wait.assert_awaited_once_with()
    assert delivery.calls == [("nack", True)]


@pytest.mark.asyncio
async def test_unknown_failure_requeues_immediately() -> None:
    delivery = StrictDelivery()
    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("unexpected")
    wait = AsyncMock()
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "connection_pool", pool),
        patch.object(service.outage_backoff, "wait", wait),
    ):
        result = await service.on_data_message(delivery, "artists")  # type: ignore[arg-type]
    assert result == DeliveryResult(Settlement.REQUEUE, "transient", "RuntimeError")
    wait.assert_not_awaited()
    assert delivery.calls == [("nack", True)]


@pytest.mark.asyncio
async def test_defer_and_cancellation_have_no_terminal_call() -> None:
    deferred = StrictDelivery()
    with patch.object(service, "shutdown_requested", True):
        result = await service.on_data_message(deferred, "artists")  # type: ignore[arg-type]
    assert result == DeliveryResult(Settlement.DEFER, "deferred")
    assert deferred.calls == []

    cancelled = StrictDelivery()
    processor = AsyncMock(side_effect=asyncio.CancelledError)
    pool, _ = _pool()
    classifier = MagicMock(wraps=service._MusicBrainzFailureClassifier("artists"))
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "connection_pool", pool),
        patch.dict(service.PROCESSORS, {"artists": processor}),
        patch.object(service, "_MusicBrainzFailureClassifier", return_value=classifier),
        pytest.raises(asyncio.CancelledError),
    ):
        await service.on_data_message(cancelled, "artists")  # type: ignore[arg-type]
    assert cancelled.calls == []
    classifier.assert_not_called()
    classifier.wait_before_requeue.assert_not_called()


@pytest.mark.asyncio
async def test_settlement_failure_is_visible_without_false_success_or_retry() -> None:
    failure = RuntimeError("broker settlement failed")
    delivery = StrictDelivery(terminal_error=failure)
    processor = AsyncMock()
    pool, _ = _pool()
    with (
        patch.object(service, "shutdown_requested", False),
        patch.object(service, "connection_pool", pool),
        patch.dict(service.PROCESSORS, {"artists": processor}),
        patch.object(service, "_record_pipeline_message") as pipeline_metric,
        patch.object(service, "_record_consumed_message") as consumed_metric,
        pytest.raises(RuntimeError, match="broker settlement failed"),
    ):
        await service.on_data_message(delivery, "artists")  # type: ignore[arg-type]
    assert delivery.calls == [("ack", None)]
    pipeline_metric.assert_not_called()
    consumed_metric.assert_not_called()


@pytest.mark.parametrize(
    ("error", "kind", "throttled"),
    [
        (InterfaceError("interface"), FailureKind.TRANSIENT, True),
        (OperationalError("operation"), FailureKind.TRANSIENT, True),
        (service.DatabaseUnavailableError("pool"), FailureKind.TRANSIENT, True),
        (DataError("data"), FailureKind.DETERMINISTIC, False),
        (IntegrityError("integrity"), FailureKind.DETERMINISTIC, False),
        (RuntimeError("unknown"), FailureKind.TRANSIENT, False),
    ],
)
def test_local_failure_classifier_preserves_policy(error: Exception, kind: FailureKind, throttled: bool) -> None:
    classifier = service._MusicBrainzFailureClassifier("artists")
    assert classifier(error) is kind
    assert classifier._throttle is throttled


def test_runtime_pin_and_private_batch_boundary_are_static() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text()
    lockfile = (ROOT / "uv.lock").read_text()
    source = (ROOT / "brainztableinator" / "brainztableinator.py").read_text()
    processor = (ROOT / "brainztableinator" / "_record_processing.py").read_text()

    assert pyproject.count(RUNTIME_REVISION) == 1
    assert lockfile.count(RUNTIME_REVISION) == 3
    assert "common.batch" not in source
    assert "AsyncBatchEngine" not in source
    assert "class BatchObserver" in processor
    assert "class _RuntimeBatchObserver" in source
