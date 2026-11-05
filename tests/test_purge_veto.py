"""The MusicBrainz purge veto, from every source, and its consecutive count."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common import WriteAdmission

import brainztableinator.brainztableinator as service
from brainztableinator._purge_veto import consecutive_vetoes, extraction_key
from brainztableinator._reconciliation import StaleChildRowPurge


SIGNAL = {"type": "extraction_complete", "version": "20261101", "started_at": "2026-11-02T00:00:00+00:00"}


def purge() -> StaleChildRowPurge:
    return StaleChildRowPurge(MagicMock(), MagicMock(), expected_data_types=("artists", "labels"), admission=WriteAdmission(1))


@pytest.mark.asyncio
async def test_a_dead_letter_on_the_broker_marks_its_type() -> None:
    target = purge()

    async def waiting(data_type: str) -> int:
        return 3 if data_type == "labels" else 0

    with patch.object(service, "dead_letters_waiting", waiting):
        await service._settle_purge_veto(target, SIGNAL)
    assert target.dead_lettered == {"labels"}


@pytest.mark.asyncio
async def test_a_broker_that_cannot_answer_marks_every_type() -> None:
    target = purge()
    with patch.object(service, "dead_letters_waiting", AsyncMock(return_value=None)):
        await service._settle_purge_veto(target, SIGNAL)
    assert target.dead_lettered == {"artists", "labels"}


@pytest.mark.asyncio
async def test_a_veto_on_the_latch_holds_for_a_redelivery() -> None:
    target = purge()
    record = AsyncMock()
    with (
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "recorded_vetoes", AsyncMock(return_value=["artists"])),
        patch.object(service, "record_extraction", record),
    ):
        await service._settle_purge_veto(target, SIGNAL)
    assert target.dead_lettered == {"artists"}
    assert record.await_args.args[1:] == ("20261101", ["artists"])


@pytest.mark.asyncio
async def test_a_clean_extraction_is_recorded_too_so_it_can_end_a_run_of_vetoes() -> None:
    record = AsyncMock()
    with patch.object(service, "connection_pool", MagicMock()), patch.object(service, "record_extraction", record):
        await service._settle_purge_veto(purge(), SIGNAL)
    assert record.await_args.args[1:] == ("20261101", [])


@pytest.mark.asyncio
async def test_a_veto_that_cannot_be_recorded_requeues_the_signal() -> None:
    target = MagicMock(spec=StaleChildRowPurge)
    target.is_latched.return_value = True
    target.is_settled = False
    target.expected_data_types = frozenset(("artists",))
    target.dead_lettered = frozenset()
    target.purge = AsyncMock()
    with (
        patch.object(service, "stale_row_purge", target),
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "record_extraction", AsyncMock(side_effect=OSError("down"))),
    ):
        assert await service._reconcile_deleted_child_rows("artists", SIGNAL) is False
    target.purge.assert_not_awaited()


@pytest.mark.parametrize(
    ("data", "expected"),
    [(SIGNAL, "20261101"), ({"started_at": "2026-11-02"}, "2026-11-02"), ({}, None), ({"version": " "}, None)],
)
def test_the_extraction_is_keyed_by_the_signals_version(data: dict[str, str], expected: str | None) -> None:
    assert extraction_key(data) == expected


class _Cursor:
    def __init__(self, rows: list[tuple[list[str]]]) -> None:
        self.rows = rows
        self.execute = AsyncMock()

    async def fetchall(self) -> list[tuple[list[str]]]:
        return self.rows


def _pool(rows: list[tuple[list[str]]]) -> MagicMock:
    conn = MagicMock()
    conn.set_autocommit = AsyncMock()
    conn.transaction.return_value.__aenter__ = AsyncMock()
    conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=_Cursor(rows))
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "expected"),
    [([], 0), ([(["artists"],)], 1), ([(["artists"],), (["labels"],), ([],), (["artists"],)], 2), ([([],), (["artists"],)], 0)],
    ids=["no-history", "one", "stops-at-a-clean-extraction", "clean-newest"],
)
async def test_consecutive_vetoes_count_back_from_the_newest_extraction(rows: list[tuple[list[str]]], expected: int) -> None:
    assert await consecutive_vetoes(_pool(rows)) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(("count", "alerts"), [(0, 0), (1, 0), (2, 1), (5, 1)])
async def test_the_alert_fires_at_the_stated_count(count: int, alerts: int) -> None:
    with (
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "consecutive_vetoes", AsyncMock(return_value=count)),
        patch.object(service, "_record_purge_consecutive_vetoes") as gauge,
        patch.object(service, "logger") as logger,
    ):
        await service._report_purge_vetoes()
    gauge.assert_called_once_with(count)
    assert logger.error.call_count == alerts
