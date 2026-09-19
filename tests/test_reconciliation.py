"""Unit tests for the MusicBrainz child-row delete-reconciliation.

The two behaviours that make the purge safe are the ones tested hardest here: the
delete-fraction cap, which refuses a shrink too large to be a real upstream
deletion, and the dead-letter veto, which refuses to run at all when a record that
is still present upstream was rejected without being upserted. Both are copied
from ``discogs-sql-loader``'s ``purge_stale_rows``; a regression in either deletes
live rows.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from common import DeliveryResult, Settlement

import brainztableinator.brainztableinator as service
from brainztableinator._reconciliation import (
    DEFAULT_MAX_DELETE_FRACTION,
    RECONCILED_TABLES,
    StaleChildRowPurge,
    ensure_reconciliation_columns,
)


if TYPE_CHECKING:
    from collections.abc import Sequence


RUN_STARTED_AT = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)

RELATIONSHIPS = "musicbrainz.relationships"
EXTERNAL_LINKS = "musicbrainz.external_links"


def _statements(mock_cursor: MagicMock) -> list[str]:
    """Return every statement the purge issued, rendered as SQL text."""
    return [_render(call.args[0]) for call in mock_cursor.execute.await_args_list]


def _render(statement: Any) -> str:
    as_string = getattr(statement, "as_string", None)
    return str(as_string()) if as_string is not None else str(statement)


def _deletes(mock_cursor: MagicMock) -> list[str]:
    return [statement for statement in _statements(mock_cursor) if statement.startswith("DELETE FROM")]


def _script_counts(mock_cursor: MagicMock, counts: Sequence[tuple[int, int]], rowcounts: Sequence[int]) -> None:
    """Answer each table's (total, stale) count pair, then its DELETE rowcount.

    ``rowcount`` is a single attribute on a real cursor, re-read after each DELETE, so
    a table whose DELETE never runs simply contributes no entry to ``rowcounts``.
    """
    fetches: list[tuple[int]] = []
    for total, stale in counts:
        fetches.append((total,))
        if total:
            fetches.append((stale,))
    mock_cursor.fetchone.side_effect = fetches

    remaining = list(rowcounts)

    def on_execute(statement: Any, *_args: Any, **_kwargs: Any) -> None:
        if _render(statement).startswith("DELETE FROM"):
            mock_cursor.rowcount = remaining.pop(0) if remaining else 0

    mock_cursor.execute.side_effect = on_execute


def _purge(mock_async_pool: MagicMock, **kwargs: Any) -> StaleChildRowPurge:
    purge = StaleChildRowPurge(mock_async_pool, MagicMock(), **kwargs)
    # The live latch reads NOW() from a server; every other test here sets the boundary directly.
    purge._run_started_at = RUN_STARTED_AT
    return purge


class TestDeleteFractionCap:
    """The cap refuses a shrink larger than a real upstream deletion could be."""

    @pytest.mark.asyncio
    async def test_purge_refuses_when_the_delete_fraction_reaches_the_cap(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """900 stale of 1000 is exactly the 90% cap, and the cap is inclusive."""
        _script_counts(mock_cursor, [(1000, 900), (10, 1)], rowcounts=[1])
        purge = _purge(mock_async_pool)

        deleted = await purge.purge(processed_records=5000)

        assert deleted[RELATIONSHIPS] == 0
        assert all("relationships" not in statement for statement in _deletes(mock_cursor))

    @pytest.mark.asyncio
    async def test_purge_deletes_when_the_delete_fraction_stays_under_the_cap(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """899 stale of 1000 is under the cap, so the DELETE runs and is reported."""
        _script_counts(mock_cursor, [(1000, 899), (0, 0)], rowcounts=[899])
        purge = _purge(mock_async_pool)

        deleted = await purge.purge(processed_records=5000)

        assert deleted[RELATIONSHIPS] == 899
        assert _deletes(mock_cursor) == ['DELETE FROM "musicbrainz"."relationships" WHERE updated_at < %s']

    @pytest.mark.asyncio
    async def test_the_cap_is_per_table_so_one_refusal_does_not_block_the_other(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """A relationships shrink past the cap must not stop external_links reconciling."""
        _script_counts(mock_cursor, [(100, 95), (100, 4)], rowcounts=[4])
        purge = _purge(mock_async_pool)

        deleted = await purge.purge(processed_records=5000)

        assert deleted == {RELATIONSHIPS: 0, EXTERNAL_LINKS: 4}

    @pytest.mark.asyncio
    async def test_the_cap_is_configurable_below_the_default(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """A tighter cap refuses a fraction the default would have allowed."""
        _script_counts(mock_cursor, [(100, 50), (0, 0)], rowcounts=[])
        purge = _purge(mock_async_pool, max_delete_fraction=0.5)

        deleted = await purge.purge(processed_records=5000)

        assert deleted[RELATIONSHIPS] == 0
        assert DEFAULT_MAX_DELETE_FRACTION == 0.9

    @pytest.mark.asyncio
    async def test_an_empty_table_is_reported_without_counting_stale_rows(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """count(*) == 0 short-circuits before the division the cap needs."""
        _script_counts(mock_cursor, [(0, 0), (0, 0)], rowcounts=[])
        purge = _purge(mock_async_pool)

        deleted = await purge.purge(processed_records=5000)

        assert deleted == {RELATIONSHIPS: 0, EXTERNAL_LINKS: 0}
        assert _deletes(mock_cursor) == []


class TestDeadLetterVeto:
    """A dead-lettered record was never upserted, so its row only looks stale."""

    @pytest.mark.asyncio
    async def test_a_dead_letter_this_run_vetoes_the_whole_purge(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """Not one statement is issued, on either table."""
        _script_counts(mock_cursor, [(1000, 1), (1000, 1)], rowcounts=[1, 1])
        purge = _purge(mock_async_pool)
        purge.record_dead_letter("artists")

        deleted = await purge.purge(processed_records=5000)

        assert deleted == {}
        mock_cursor.execute.assert_not_awaited()
        mock_async_pool.connection.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_veto_is_global_because_every_type_writes_both_tables(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """A rejected label vetoes relationships sourced from artists too."""
        _script_counts(mock_cursor, [(1000, 1), (1000, 1)], rowcounts=[1, 1])
        purge = _purge(mock_async_pool)
        purge.record_dead_letter("labels")

        assert await purge.purge(processed_records=5000) == {}
        assert purge.dead_lettered == frozenset({"labels"})

    @pytest.mark.asyncio
    async def test_a_run_that_processed_nothing_is_vetoed(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """Zero processed records reads as a resumed extraction, not an empty dump."""
        _script_counts(mock_cursor, [(1000, 900), (1000, 900)], rowcounts=[])
        purge = _purge(mock_async_pool)

        assert await purge.purge(processed_records=0) == {}
        mock_cursor.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unlatched_run_start_is_vetoed(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """Without a run start there is no boundary, so nothing may be deleted."""
        purge = StaleChildRowPurge(mock_async_pool, MagicMock())

        assert purge.run_started_at is None
        assert await purge.purge(processed_records=5000) == {}
        mock_cursor.execute.assert_not_awaited()

    def test_reset_clears_the_run_start_and_the_dead_letter_marks(self, mock_async_pool: MagicMock) -> None:
        purge = _purge(mock_async_pool)
        purge.record_dead_letter("releases")

        purge.reset()

        assert purge.run_started_at is None
        assert purge.dead_lettered == frozenset()


class TestRunStartAndLatching:
    """Run start comes from the database clock, and the purge waits for every type."""

    @pytest.mark.asyncio
    async def test_latch_run_start_reads_the_database_clock(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """NOW() on the server is what the upserts stamp rows with, so no host clock is used."""
        mock_cursor.fetchone.return_value = (RUN_STARTED_AT,)
        purge = StaleChildRowPurge(mock_async_pool, MagicMock())

        started_at = await purge.latch_run_start()

        assert started_at == RUN_STARTED_AT
        assert purge.run_started_at == RUN_STARTED_AT
        mock_cursor.execute.assert_awaited_once_with("SELECT NOW()")

    def test_latched_for_waits_for_every_entity_type(self, mock_async_pool: MagicMock) -> None:
        """Both tables are fed by all four kinds, so a subset must not trigger the purge."""
        purge = _purge(mock_async_pool)
        expected = ["artists", "labels", "release-groups", "releases"]

        assert not purge.latched_for({"artists"}, expected)
        assert not purge.latched_for({"artists", "labels", "releases"}, expected)
        assert purge.latched_for(set(expected), expected)

    @pytest.mark.asyncio
    async def test_purge_keys_every_statement_on_the_latched_run_start(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        _script_counts(mock_cursor, [(100, 10), (100, 10)], rowcounts=[10, 10])
        purge = _purge(mock_async_pool)

        await purge.purge(processed_records=5000)

        parameterized = [call.args[1] for call in mock_cursor.execute.await_args_list if len(call.args) > 1]
        assert parameterized == [(RUN_STARTED_AT,)] * 4


class TestTransactionAndIdempotence:
    """The reconciliation is one transaction, and a second pass deletes nothing."""

    @pytest.mark.asyncio
    async def test_both_tables_reconcile_in_one_transaction(
        self,
        mock_async_pool: MagicMock,
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        _script_counts(mock_cursor, [(100, 10), (100, 10)], rowcounts=[10, 10])
        purge = _purge(mock_async_pool)

        await purge.purge(processed_records=5000)

        mock_connection.set_autocommit.assert_awaited_once_with(False)
        mock_connection.transaction.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_a_second_pass_over_reconciled_tables_deletes_nothing(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """Nothing older than the run start survives the first pass, so the second is a no-op."""
        _script_counts(mock_cursor, [(90, 0), (90, 0)], rowcounts=[])
        purge = _purge(mock_async_pool)

        deleted = await purge.purge(processed_records=5000)

        assert deleted == {RELATIONSHIPS: 0, EXTERNAL_LINKS: 0}
        assert _deletes(mock_cursor) == []

    @pytest.mark.asyncio
    async def test_a_failing_statement_propagates_so_the_signal_can_be_requeued(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        mock_cursor.execute.side_effect = RuntimeError("connection reset")
        purge = _purge(mock_async_pool)

        with pytest.raises(RuntimeError, match="connection reset"):
            await purge.purge(processed_records=5000)


class TestReconciliationColumns:
    """The column the purge keys on is added additively and idempotently."""

    @pytest.mark.asyncio
    async def test_ensure_adds_updated_at_to_both_tables_if_not_exists(
        self,
        mock_async_pool: MagicMock,
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        await ensure_reconciliation_columns(mock_async_pool, MagicMock())

        statements = _statements(mock_cursor)
        assert len(statements) == len(RECONCILED_TABLES)
        for statement in statements:
            assert "ADD COLUMN IF NOT EXISTS" in statement
            assert "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()" in statement
        assert any('"musicbrainz"."relationships"' in statement for statement in statements)
        assert any('"musicbrainz"."external_links"' in statement for statement in statements)
        mock_connection.set_autocommit.assert_awaited_once_with(False)


class _RecordingPurge:
    """Stand-in for the purge that records how the service drove it."""

    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[int] = []
        self.dead_lettered_types: list[str] = []
        self._error = error

    def latched_for(self, completed: Any, expected: Any) -> bool:
        return set(expected) <= set(completed)

    def record_dead_letter(self, data_type: str) -> None:
        self.dead_lettered_types.append(data_type)

    async def purge(self, processed_records: int) -> dict[str, int]:
        self.calls.append(processed_records)
        if self._error is not None:
            raise self._error
        return {RELATIONSHIPS: 3, EXTERNAL_LINKS: 1}


def _extraction_complete() -> MagicMock:
    message = MagicMock()
    message.body = b'{"type": "extraction_complete", "version": "2026-01-01"}'
    return message


class TestServiceLatching:
    """How ``extraction_complete`` and a dead letter drive the purge."""

    @pytest.mark.asyncio
    async def test_the_purge_waits_until_every_entity_type_has_completed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Purging after one type would delete the rows the other three still owe."""
        purge = _RecordingPurge()
        monkeypatch.setattr(service, "stale_row_purge", purge)
        monkeypatch.setattr(service, "completed_files", set())
        monkeypatch.setattr(service, "CONSUMER_CANCEL_DELAY", 0)

        result = await service._handle_data_message(_extraction_complete(), "artists")

        assert result == DeliveryResult(Settlement.ACK, "skipped")
        assert purge.calls == []

    @pytest.mark.asyncio
    async def test_the_last_entity_types_signal_runs_the_purge(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        purge = _RecordingPurge()
        monkeypatch.setattr(service, "stale_row_purge", purge)
        monkeypatch.setattr(service, "completed_files", {"artists", "labels", "release-groups"})
        monkeypatch.setattr(service, "message_counts", {"artists": 7, "labels": 2, "release-groups": 1, "releases": 5})
        monkeypatch.setattr(service, "CONSUMER_CANCEL_DELAY", 0)

        result = await service._handle_data_message(_extraction_complete(), "releases")

        assert result == DeliveryResult(Settlement.ACK, "skipped")
        assert purge.calls == [15]

    @pytest.mark.asyncio
    async def test_a_failed_purge_requeues_the_signal_and_withdraws_completion(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The reconciliation is the run's only chance to delete, so it is retried."""
        purge = _RecordingPurge(error=RuntimeError("deadlock detected"))
        completed = {"artists", "labels", "release-groups"}
        monkeypatch.setattr(service, "stale_row_purge", purge)
        monkeypatch.setattr(service, "completed_files", completed)
        monkeypatch.setattr(service, "message_counts", dict.fromkeys(service.MUSICBRAINZ_DATA_TYPES, 1))
        monkeypatch.setattr(service, "CONSUMER_CANCEL_DELAY", 0)

        result = await service._handle_data_message(_extraction_complete(), "releases")

        assert result == DeliveryResult(Settlement.REQUEUE, "failed", "ReconciliationFailed")
        assert "releases" not in completed

    @pytest.mark.asyncio
    async def test_a_rejected_delivery_is_recorded_as_a_dead_letter(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A non-UUID id is rejected to the DLQ, which must veto this run's purge."""
        purge = _RecordingPurge()
        monkeypatch.setattr(service, "stale_row_purge", purge)
        monkeypatch.setattr(service, "shutdown_requested", False)
        message = AsyncMock()
        message.body = b'{"id": "not-a-uuid", "name": "Artist"}'
        message.headers = {}

        result = await service.on_data_message(message, "artists")

        assert result.settlement is Settlement.REJECT
        assert purge.dead_lettered_types == ["artists"]

    @pytest.mark.asyncio
    async def test_an_acked_delivery_is_not_recorded_as_a_dead_letter(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        purge = _RecordingPurge()
        monkeypatch.setattr(service, "stale_row_purge", purge)
        monkeypatch.setattr(service, "completed_files", set())
        monkeypatch.setattr(service, "shutdown_requested", False)
        monkeypatch.setattr(service, "CONSUMER_CANCEL_DELAY", 0)
        message = AsyncMock()
        message.body = b'{"type": "file_complete", "total_processed": 1}'
        message.headers = {}

        await service.on_data_message(message, "artists")

        assert purge.dead_lettered_types == []

    @pytest.mark.asyncio
    async def test_an_unavailable_purge_leaves_the_signal_acked(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Startup failing to wire the purge must not stop the loader from loading."""
        monkeypatch.setattr(service, "stale_row_purge", None)
        monkeypatch.setattr(service, "completed_files", set(service.MUSICBRAINZ_DATA_TYPES))
        monkeypatch.setattr(service, "CONSUMER_CANCEL_DELAY", 0)

        result = await service._handle_data_message(_extraction_complete(), "releases")

        assert result == DeliveryResult(Settlement.ACK, "skipped")
