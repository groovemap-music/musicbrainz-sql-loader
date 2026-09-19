"""Unit tests for the MusicBrainz child-row delete-reconciliation.

Four behaviours make the purge safe, and each is tested hardest here: the
delete-fraction cap, which refuses a shrink too large to be a real upstream
deletion; the dead-letter veto, which refuses to run at all when a record that is
still present upstream was rejected without being upserted; the startup probe,
which keeps the whole feature off until the schema owner has declared the column;
and the boundary, which comes from the ``extraction_complete`` body so a loader
restart cannot move it forward onto rows the run itself wrote.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from common import DeliveryResult, Settlement

import brainztableinator.brainztableinator as service
from brainztableinator._persistence import PostgreSQLMusicBrainzWriter
from brainztableinator._reconciliation import (
    DEFAULT_MAX_DELETE_FRACTION,
    RECONCILED_TABLES,
    RECONCILIATION_COLUMN,
    StaleChildRowPurge,
    parse_started_at,
    reconciliation_columns_present,
)


if TYPE_CHECKING:
    from collections.abc import Sequence


BOUNDARY = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)
LATER_BOUNDARY = BOUNDARY + timedelta(days=7)

DATA_TYPES = ("artists", "labels", "release-groups", "releases")

RELATIONSHIPS = "musicbrainz.relationships"
EXTERNAL_LINKS = "musicbrainz.external_links"


def _signal(started_at: datetime = BOUNDARY, counts: int = 1000) -> dict[str, Any]:
    """Build an ``extraction_complete`` body with every field the v1 schema requires."""
    return {
        "type": "extraction_complete",
        "version": "2026-09-18",
        "timestamp": started_at.isoformat(),
        "started_at": started_at.isoformat(),
        "record_counts": dict.fromkeys(DATA_TYPES, counts),
    }


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


def _latched(mock_async_pool: MagicMock, started_at: datetime = BOUNDARY, **kwargs: Any) -> StaleChildRowPurge:
    """Return a purge with all four signals collected for one boundary."""
    purge = StaleChildRowPurge(mock_async_pool, MagicMock(), **kwargs)
    for data_type in DATA_TYPES:
        purge.record_completion(data_type, _signal(started_at))
    assert purge.is_latched()
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
        purge = _latched(mock_async_pool)

        deleted = await purge.purge()

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
        purge = _latched(mock_async_pool)

        deleted = await purge.purge()

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
        purge = _latched(mock_async_pool)

        deleted = await purge.purge()

        assert deleted == {RELATIONSHIPS: 0, EXTERNAL_LINKS: 4}

    @pytest.mark.asyncio
    async def test_the_cap_is_configurable_below_the_default(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """A tighter cap refuses a fraction the default would have allowed."""
        _script_counts(mock_cursor, [(100, 50), (0, 0)], rowcounts=[])
        purge = _latched(mock_async_pool, max_delete_fraction=0.5)

        deleted = await purge.purge()

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
        purge = _latched(mock_async_pool)

        deleted = await purge.purge()

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
        purge = _latched(mock_async_pool)
        purge.record_dead_letter("artists")

        deleted = await purge.purge()

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
        purge = _latched(mock_async_pool)
        purge.record_dead_letter("labels")

        assert await purge.purge() == {}
        mock_cursor.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_marks_are_cleared_once_the_extraction_they_belong_to_concludes(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """Mirrors reset_dlq_nacks: run N's poison must not veto run N+1 forever."""
        _script_counts(mock_cursor, [(100, 10), (100, 10)], rowcounts=[10, 10])
        purge = _latched(mock_async_pool)
        purge.record_dead_letter("artists")

        assert await purge.purge() == {}
        assert purge.dead_lettered == frozenset()

        for data_type in DATA_TYPES:
            purge.record_completion(data_type, _signal(LATER_BOUNDARY))
        assert await purge.purge() == {RELATIONSHIPS: 10, EXTERNAL_LINKS: 10}

    @pytest.mark.asyncio
    async def test_a_type_the_extractor_reported_no_records_for_is_vetoed(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """Zero records for a type reads as a resumed extraction, not an empty dump."""
        _script_counts(mock_cursor, [(1000, 900), (1000, 900)], rowcounts=[])
        purge = StaleChildRowPurge(mock_async_pool, MagicMock())
        for data_type in DATA_TYPES:
            message = _signal()
            if data_type == "labels":
                message["record_counts"]["labels"] = 0
            purge.record_completion(data_type, message)

        assert await purge.purge() == {}
        mock_cursor.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_missing_record_count_is_vetoed(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        purge = StaleChildRowPurge(mock_async_pool, MagicMock())
        for data_type in DATA_TYPES:
            message = _signal()
            message["record_counts"] = {}
            purge.record_completion(data_type, message)

        assert await purge.purge() == {}
        mock_cursor.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unlatched_purge_deletes_nothing(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """Without all four signals there is no boundary anyone agreed on."""
        purge = StaleChildRowPurge(mock_async_pool, MagicMock())
        purge.record_completion("artists", _signal())

        assert not purge.is_latched()
        assert await purge.purge() == {}
        mock_cursor.execute.assert_not_awaited()

    def test_reset_clears_the_boundary_the_signals_and_the_marks(self, mock_async_pool: MagicMock) -> None:
        purge = _latched(mock_async_pool)
        purge.record_dead_letter("releases")

        purge.reset()

        assert purge.boundary is None
        assert purge.signalled == frozenset()
        assert purge.dead_lettered == frozenset()


class TestBoundaryComesFromTheMessage:
    """The boundary is the extraction's own start, so a restart cannot move it."""

    def test_the_boundary_is_the_messages_started_at(self, mock_async_pool: MagicMock) -> None:
        purge = _latched(mock_async_pool)

        assert purge.boundary == BOUNDARY

    def test_a_naive_started_at_is_read_as_utc(self) -> None:
        assert parse_started_at("2026-09-18T12:00:00") == BOUNDARY
        assert parse_started_at("2026-09-18T12:00:00+00:00") == BOUNDARY

    @pytest.mark.parametrize("value", ["", "not-a-timestamp", None, 17, {}])
    def test_an_unusable_started_at_is_rejected_rather_than_guessed_at(self, value: Any) -> None:
        assert parse_started_at(value) is None

    @pytest.mark.asyncio
    async def test_an_unusable_started_at_drops_the_latch(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """The value becomes a DELETE boundary, so a bad one must stop the purge."""
        purge = _latched(mock_async_pool)
        purge.record_completion("artists", {"type": "extraction_complete"})

        assert purge.boundary is None
        assert not purge.is_latched()
        assert await purge.purge() == {}
        mock_cursor.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_restart_mid_run_does_not_purge_the_rows_that_run_wrote(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """The regression the earlier SELECT NOW() boundary caused.

        A fresh process, standing in for the loader after a restart, must still key the
        DELETE on the extraction's own start. Had the boundary come from process start,
        it would sit after everything the run wrote before the restart and delete it.
        """
        _script_counts(mock_cursor, [(100, 10), (100, 10)], rowcounts=[10, 10])
        restarted = StaleChildRowPurge(mock_async_pool, MagicMock())
        for data_type in DATA_TYPES:
            restarted.record_completion(data_type, _signal())

        await restarted.purge()

        parameterized = [call.args[1] for call in mock_cursor.execute.await_args_list if len(call.args) > 1]
        assert parameterized == [(BOUNDARY,)] * 4

    @pytest.mark.asyncio
    async def test_a_later_extraction_re_latches_instead_of_re_firing_the_first(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """A long-lived loader sees more than one extraction; the second must collect afresh."""
        _script_counts(mock_cursor, [(100, 10), (100, 10)], rowcounts=[10, 10])
        purge = _latched(mock_async_pool)
        await purge.purge()
        mock_cursor.reset_mock()
        _script_counts(mock_cursor, [(100, 5), (100, 5)], rowcounts=[5, 5])

        # The next extraction's first signal must not re-fire the previous purge.
        purge.record_completion("artists", _signal(LATER_BOUNDARY))
        assert purge.boundary == LATER_BOUNDARY
        assert purge.signalled == frozenset({"artists"})
        assert not purge.is_latched()
        assert await purge.purge() == {}
        mock_cursor.execute.assert_not_awaited()

        for data_type in DATA_TYPES[1:]:
            purge.record_completion(data_type, _signal(LATER_BOUNDARY))

        assert await purge.purge() == {RELATIONSHIPS: 5, EXTERNAL_LINKS: 5}
        parameterized = [call.args[1] for call in mock_cursor.execute.await_args_list if len(call.args) > 1]
        assert parameterized == [(LATER_BOUNDARY,)] * 4

    def test_a_redelivered_signal_from_a_finished_extraction_is_ignored(
        self,
        mock_async_pool: MagicMock,
    ) -> None:
        """A straggler must not drag the boundary backwards onto the current run's rows."""
        purge = StaleChildRowPurge(mock_async_pool, MagicMock())
        for data_type in DATA_TYPES:
            purge.record_completion(data_type, _signal(LATER_BOUNDARY))

        purge.record_completion("artists", _signal(BOUNDARY))

        assert purge.boundary == LATER_BOUNDARY
        assert purge.is_latched()

    @pytest.mark.asyncio
    async def test_the_same_extraction_is_not_reconciled_twice(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """Redelivering all four signals for a reconciled boundary issues no statement."""
        _script_counts(mock_cursor, [(100, 10), (100, 10)], rowcounts=[10, 10])
        purge = _latched(mock_async_pool)
        assert await purge.purge() == {RELATIONSHIPS: 10, EXTERNAL_LINKS: 10}
        mock_cursor.reset_mock()

        assert await purge.purge() == {}
        mock_cursor.execute.assert_not_awaited()


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
        purge = _latched(mock_async_pool)

        await purge.purge()

        mock_connection.set_autocommit.assert_awaited_once_with(False)
        mock_connection.transaction.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_a_second_extraction_over_reconciled_tables_deletes_nothing(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        """Nothing older than the boundary survives the first pass, so the next is a no-op."""
        _script_counts(mock_cursor, [(90, 0), (90, 0)], rowcounts=[])
        purge = _latched(mock_async_pool)

        deleted = await purge.purge()

        assert deleted == {RELATIONSHIPS: 0, EXTERNAL_LINKS: 0}
        assert _deletes(mock_cursor) == []

    @pytest.mark.asyncio
    async def test_a_failing_statement_propagates_so_the_signal_can_be_requeued(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        mock_cursor.execute.side_effect = RuntimeError("connection reset")
        purge = _latched(mock_async_pool)

        with pytest.raises(RuntimeError, match="connection reset"):
            await purge.purge()


class TestStartupProbe:
    """The loader issues no DDL; it probes for the column database-schema owns."""

    @pytest.mark.asyncio
    async def test_the_probe_reads_information_schema_for_both_tables(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        mock_cursor.fetchone.side_effect = [(1,), (1,)]

        assert await reconciliation_columns_present(mock_async_pool, MagicMock()) is True

        statements = _statements(mock_cursor)
        assert len(statements) == len(RECONCILED_TABLES)
        for statement in statements:
            assert "information_schema.columns" in statement
        parameters = [call.args[1] for call in mock_cursor.execute.await_args_list]
        assert parameters == [
            ("musicbrainz", "relationships", RECONCILIATION_COLUMN),
            ("musicbrainz", "external_links", RECONCILIATION_COLUMN),
        ]

    @pytest.mark.asyncio
    async def test_the_probe_issues_no_ddl(self, mock_async_pool: MagicMock, mock_cursor: MagicMock) -> None:
        """database-schema is the only repository that issues DDL against these tables."""
        mock_cursor.fetchone.side_effect = [(0,), (0,)]

        await reconciliation_columns_present(mock_async_pool, MagicMock())

        for statement in _statements(mock_cursor):
            assert "ALTER TABLE" not in statement.upper()
            assert "CREATE " not in statement.upper()

    @pytest.mark.asyncio
    async def test_an_absent_column_disables_the_feature_with_one_log_line(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        mock_cursor.fetchone.side_effect = [(0,), (0,)]
        probe_logger = MagicMock()

        assert await reconciliation_columns_present(mock_async_pool, probe_logger) is False

        probe_logger.warning.assert_called_once()
        assert RECONCILIATION_COLUMN in probe_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_one_missing_table_is_enough_to_disable_the_feature(
        self,
        mock_async_pool: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        mock_cursor.fetchone.side_effect = [(1,), (0,)]

        assert await reconciliation_columns_present(mock_async_pool, MagicMock()) is False


class TestConditionalUpsertClause:
    """The upsert names `updated_at` only once the probe has found it."""

    def test_the_default_writer_names_no_column_the_schema_has_not_declared(self) -> None:
        writer = PostgreSQLMusicBrainzWriter()

        assert writer.refresh_updated_at is False

    @pytest.mark.asyncio
    async def test_the_clause_is_absent_until_the_refresh_is_enabled(
        self,
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        writer = PostgreSQLMusicBrainzWriter()

        await writer.insert_relationships(mock_connection, [("s", "artist", "t", "artist", "member of band", [], None, None, False)])
        await writer.insert_external_links(mock_connection, [("m", "artist", "https://example.invalid", "homepage")])

        for call in mock_cursor.executemany.await_args_list:
            assert "updated_at" not in call.args[0]

    @pytest.mark.asyncio
    async def test_the_clause_is_present_once_the_refresh_is_enabled(
        self,
        mock_connection: MagicMock,
        mock_cursor: MagicMock,
    ) -> None:
        writer = PostgreSQLMusicBrainzWriter()
        writer.set_refresh_updated_at(True)

        await writer.insert_relationships(mock_connection, [("s", "artist", "t", "artist", "member of band", [], None, None, False)])
        await writer.insert_external_links(mock_connection, [("m", "artist", "https://example.invalid", "homepage")])

        statements = [call.args[0] for call in mock_cursor.executemany.await_args_list]
        assert all(statement.endswith("updated_at = NOW()") for statement in statements)
        assert "DO UPDATE SET ended = EXCLUDED.ended, updated_at = NOW()" in statements[0]
        assert "DO UPDATE SET url = EXCLUDED.url, updated_at = NOW()" in statements[1]


class _RecordingPurge:
    """Stand-in for the purge that records how the service drove it."""

    def __init__(self, error: Exception | None = None, latched: bool = True) -> None:
        self.calls: int = 0
        self.completions: list[tuple[str, Any]] = []
        self.dead_lettered_types: list[str] = []
        self.boundary = BOUNDARY
        self._error = error
        self._latched = latched

    def record_completion(self, data_type: str, message: Any) -> None:
        self.completions.append((data_type, message.get("started_at")))

    def is_latched(self) -> bool:
        return self._latched

    def pending_data_types(self) -> list[str]:
        return [] if self._latched else ["releases"]

    @property
    def signalled(self) -> frozenset[str]:
        return frozenset(data_type for data_type, _ in self.completions)

    def record_dead_letter(self, data_type: str) -> None:
        self.dead_lettered_types.append(data_type)

    async def purge(self) -> dict[str, int]:
        self.calls += 1
        if self._error is not None:
            raise self._error
        return {RELATIONSHIPS: 3, EXTERNAL_LINKS: 1}


def _extraction_complete_message() -> MagicMock:
    import orjson

    message = MagicMock()
    message.body = orjson.dumps(_signal())
    return message


class TestServiceLatching:
    """How ``extraction_complete`` and a dead letter drive the purge."""

    @pytest.mark.asyncio
    async def test_the_signal_is_recorded_with_its_own_started_at(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        purge = _RecordingPurge(latched=False)
        monkeypatch.setattr(service, "stale_row_purge", purge)
        monkeypatch.setattr(service, "completed_files", set())
        monkeypatch.setattr(service, "CONSUMER_CANCEL_DELAY", 0)

        result = await service._handle_data_message(_extraction_complete_message(), "artists")

        assert result == DeliveryResult(Settlement.ACK, "skipped")
        assert purge.completions == [("artists", BOUNDARY.isoformat())]
        assert purge.calls == 0

    @pytest.mark.asyncio
    async def test_the_last_entity_types_signal_runs_the_purge(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        purge = _RecordingPurge()
        monkeypatch.setattr(service, "stale_row_purge", purge)
        monkeypatch.setattr(service, "completed_files", set())
        monkeypatch.setattr(service, "CONSUMER_CANCEL_DELAY", 0)

        result = await service._handle_data_message(_extraction_complete_message(), "releases")

        assert result == DeliveryResult(Settlement.ACK, "skipped")
        assert purge.calls == 1

    @pytest.mark.asyncio
    async def test_the_purge_no_longer_reads_completed_files(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """completed_files stays full across extractions, so the latch owns its own state."""
        purge = _RecordingPurge(latched=False)
        monkeypatch.setattr(service, "stale_row_purge", purge)
        monkeypatch.setattr(service, "completed_files", set(service.MUSICBRAINZ_DATA_TYPES))
        monkeypatch.setattr(service, "CONSUMER_CANCEL_DELAY", 0)

        await service._handle_data_message(_extraction_complete_message(), "artists")

        assert purge.calls == 0

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
        monkeypatch.setattr(service, "CONSUMER_CANCEL_DELAY", 0)

        result = await service._handle_data_message(_extraction_complete_message(), "releases")

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
        """A probe that found no column must not stop the loader from loading."""
        monkeypatch.setattr(service, "stale_row_purge", None)
        monkeypatch.setattr(service, "completed_files", set(service.MUSICBRAINZ_DATA_TYPES))
        monkeypatch.setattr(service, "CONSUMER_CANCEL_DELAY", 0)

        result = await service._handle_data_message(_extraction_complete_message(), "releases")

        assert result == DeliveryResult(Settlement.ACK, "skipped")
