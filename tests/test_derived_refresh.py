"""The derived-relation refresh on a completed MusicBrainz extraction, without a database."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from common import WriteAdmission

import brainztableinator._derived_refresh as derived
import brainztableinator.brainztableinator as service
from brainztableinator._reconciliation import StaleChildRowPurge


SIGNAL = {"type": "extraction_complete", "version": "20261101", "started_at": "2026-11-02T00:00:00+00:00"}


def _latched_purge(calls: list[str]) -> MagicMock:
    target = MagicMock(spec=StaleChildRowPurge)
    target.is_latched.return_value = True
    target.is_settled = False
    target.expected_data_types = frozenset()
    target.dead_lettered = frozenset()
    target.lost_lists = ()

    async def purge() -> dict[str, int]:
        calls.append("purge")
        return {}

    target.purge = purge
    return target


async def _run(calls: list[str], refresh: AsyncMock, *, enabled: bool = True) -> bool:
    with (
        patch.object(service, "stale_row_purge", _latched_purge(calls)),
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "write_admission", WriteAdmission(1)),
        patch.object(service, "derived_refresh_enabled", enabled),
        patch.object(service, "record_extraction", AsyncMock()),
        patch.object(service, "recorded_vetoes", AsyncMock(return_value=[])),
        patch.object(service, "dead_letters_waiting", AsyncMock(return_value=0)),
        patch.object(service, "refresh_derived_relations", refresh),
        patch.object(service, "_report_purge_vetoes", AsyncMock()),
    ):
        return await service._reconcile_deleted_child_rows("artists", SIGNAL)


@pytest.mark.asyncio
async def test_the_refresh_runs_once_after_the_purge_for_the_extraction() -> None:
    calls: list[str] = []

    async def refresh(_pool: Any, _logger: Any, version: str) -> None:
        calls.append(f"refresh:{version}")

    assert await _run(calls, AsyncMock(side_effect=refresh)) is True
    assert calls == ["purge", "refresh:20261101"]


@pytest.mark.asyncio
async def test_a_failed_refresh_requeues_the_signal_instead_of_swallowing_it() -> None:
    assert await _run([], AsyncMock(side_effect=OSError("down"))) is False


@pytest.mark.asyncio
async def test_a_failed_purge_does_not_refresh() -> None:
    refresh = AsyncMock()
    target = _latched_purge([])
    target.purge = AsyncMock(side_effect=OSError("down"))
    with (
        patch.object(service, "stale_row_purge", target),
        patch.object(service, "connection_pool", MagicMock()),
        patch.object(service, "write_admission", WriteAdmission(1)),
        patch.object(service, "derived_refresh_enabled", True),
        patch.object(service, "record_extraction", AsyncMock()),
        patch.object(service, "recorded_vetoes", AsyncMock(return_value=[])),
        patch.object(service, "dead_letters_waiting", AsyncMock(return_value=0)),
        patch.object(service, "refresh_derived_relations", refresh),
    ):
        assert await service._reconcile_deleted_child_rows("artists", SIGNAL) is False
    refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_refresh_waits_for_every_entity_type() -> None:
    refresh = AsyncMock()
    target = _latched_purge([])
    target.is_latched.return_value = False
    target.signalled = frozenset()
    target.pending_data_types.return_value = ["labels"]
    target.boundary = None
    with patch.object(service, "stale_row_purge", target), patch.object(service, "refresh_derived_relations", refresh):
        assert await service._reconcile_deleted_child_rows("artists", SIGNAL) is True
    refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_schema_without_the_refresh_relations_leaves_the_loader_loading() -> None:
    refresh = AsyncMock()
    assert await _run([], refresh, enabled=False) is True
    refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_pair_runs_in_contract_order_inside_one_transaction() -> None:
    executed: list[str] = []
    transactions: list[str] = []

    class Cursor:
        def __init__(self) -> None:
            self.rows: list[Any] = []

        async def __aenter__(self) -> Cursor:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def execute(self, sql: str, _params: Any = None) -> None:
            executed.append(sql)
            if "refresh_artist_member_of" in sql:
                self.rows = [("discogs", 3), ("musicbrainz", 5)]
            elif "refresh_vertex_degree" in sql:
                self.rows = [("a", 7)]
            elif "FROM public.loader_derived_refresh_cursor" in sql:
                self.rows = [(1, "20261101")]
            else:
                self.rows = [(1,)]

        async def fetchone(self) -> Any:
            return self.rows[0]

        async def fetchall(self) -> list[Any]:
            return self.rows

    class Transaction:
        async def __aenter__(self) -> None:
            transactions.append("begin")

        async def __aexit__(self, *_: object) -> None:
            transactions.append("commit")

    class Conn:
        set_autocommit = AsyncMock()

        def transaction(self) -> Transaction:
            return Transaction()

        def cursor(self) -> Cursor:
            return Cursor()

        async def __aenter__(self) -> Conn:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

    pool = MagicMock()
    pool.connection = lambda: Conn()
    counts = await derived._refresh(pool, MagicMock(), "20261101", 1, MagicMock(), 1)
    calls = [sql for sql in executed if "graph.refresh_" in sql]
    assert [("member_of" in sql) for sql in calls] == [True, False]
    assert counts == {"refresh_artist_member_of": {"discogs": 3, "musicbrainz": 5}, "refresh_vertex_degree": {"a": 7}}
    assert transactions == ["begin", "commit"]
    assert any("state = 'completed'" in sql for sql in executed)
    assert any("SET refreshed_at" in sql for sql in executed)


@pytest.mark.asyncio
async def test_an_already_refreshed_extraction_is_not_refreshed_again() -> None:
    with patch.object(derived, "_claim", AsyncMock(return_value=None)), patch.object(derived, "_refresh", AsyncMock()) as refresh:
        assert await derived.refresh_derived_relations(MagicMock(), MagicMock(), "20261101") is None
    refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_failed_pass_is_marked_for_retry_and_raised() -> None:
    mark = AsyncMock()
    with (
        patch.object(derived, "_claim", AsyncMock(return_value=(2, MagicMock(), 1))),
        patch.object(derived, "_refresh", AsyncMock(side_effect=OSError("down"))),
        patch.object(derived, "_mark_retry", mark),
        pytest.raises(OSError, match="down"),
    ):
        await derived.refresh_derived_relations(MagicMock(), MagicMock(), "20261101")
    assert mark.await_args.args[1] == "20261101"
    assert "down" in mark.await_args.args[-1]


@pytest.mark.asyncio
async def test_a_superseded_pass_stamps_nothing_and_does_not_requeue() -> None:
    with (
        patch.object(derived, "_claim", AsyncMock(return_value=(2, MagicMock(), 1))),
        patch.object(derived, "_refresh", AsyncMock(side_effect=derived.SupersededRefresh("newer"))),
        patch.object(derived, "_mark_retry", AsyncMock()) as mark,
    ):
        assert await derived.refresh_derived_relations(MagicMock(), MagicMock(), "20261101") is None
    mark.assert_not_awaited()
