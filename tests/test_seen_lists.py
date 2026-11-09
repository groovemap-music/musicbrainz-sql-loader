"""Unit coverage for the reload seen lists: the mode, the probe, the appends, and the purges.

The behaviour against a real schema (that an unchanged reload writes nothing, that the shadow
comparison matches, that a lost list purges nothing) is asserted in
``tests/integration/test_reload_seen_lists.py``. These tests need no database: they pin which
statements each mode issues, in which order, and with which parameters.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from common import WriteAdmission
from psycopg.types.json import Jsonb

import brainztableinator.brainztableinator as service
from brainztableinator._persistence import (
    EXTERNAL_LINK_SEEN_INSERT,
    EXTERNAL_LINK_UPSERTS,
    RELATIONSHIP_SEEN_INSERT,
    RELATIONSHIP_SEEN_UPDATE,
    RELATIONSHIP_UPSERTS,
    PostgreSQLMusicBrainzWriter,
)
from brainztableinator._reconciliation import StaleChildRowPurge
from brainztableinator._record_processing import _stored_data
from brainztableinator._seen_lists import (
    DEFAULT_RELOAD_MODE,
    EXTERNAL_LINK_SEEN_APPEND,
    RELATIONSHIP_SEEN_APPEND,
    ReloadMode,
    missing_seen_list_objects,
    parse_reload_mode,
    resolve_reload_mode,
)
from brainztableinator.config import MusicBrainzSQLLoaderConfig


BOUNDARY = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
DATA_TYPES = ("artists", "labels", "release-groups", "releases")

SOURCE = "00000000-0000-0000-0000-00000000a001"
TARGET = "00000000-0000-0000-0000-00000000b002"

# (source_mbid, source_entity_type, target_mbid, target_entity_type, relationship_type, attributes,
# begin_date, end_date, ended), as `_record_processing.insert_relationships` builds it.
RELATIONSHIP = (SOURCE, "artist", TARGET, "artist", "member of band", ["guitar"], "1990", None, True)
# (mbid, entity_type, url, service_name)
LINK = (SOURCE, "artist", "https://example.com", "official homepage")


def _render(statement: Any) -> str:
    as_string = getattr(statement, "as_string", None)
    return str(as_string()) if as_string is not None else str(statement)


def _signal(started_at: datetime = BOUNDARY) -> dict[str, Any]:
    return {
        "type": "extraction_complete",
        "version": "2026-11-01",
        "timestamp": started_at.isoformat(),
        "started_at": started_at.isoformat(),
        "record_counts": dict.fromkeys(DATA_TYPES, 1000),
    }


def _latched(mock_async_pool: MagicMock, mode: ReloadMode, logger: MagicMock | None = None) -> StaleChildRowPurge:
    purge = StaleChildRowPurge(mock_async_pool, logger or MagicMock(), admission=WriteAdmission(4), mode=mode)
    for data_type in DATA_TYPES:
        purge.record_completion(data_type, _signal())
    assert purge.is_latched()
    return purge


class _ScriptedCursor:
    """Answer each statement by the first matching fragment, and record what ran."""

    def __init__(self, mock_cursor: MagicMock, mock_connection: MagicMock, answers: dict[str, Any], fail_on: str | None = None) -> None:
        self.statements: list[tuple[str, Any]] = []
        self._answers = answers
        self._fail_on = fail_on
        self._next: Any = None
        self._cursor = mock_cursor
        mock_cursor.execute.side_effect = self._execute
        mock_cursor.fetchone.side_effect = lambda: self._next
        mock_cursor.connection = mock_connection

    def _execute(self, statement: Any, params: Any = None, **_kwargs: Any) -> None:
        text = _render(statement)
        self.statements.append((text, params))
        if self._fail_on is not None and self._fail_on in text:
            raise RuntimeError(f"scripted failure on {self._fail_on}")
        self._next = None
        for fragment, answer in self._answers.items():
            if fragment in text:
                self._next = answer(params) if callable(answer) else answer
                break
        if text.startswith("DELETE FROM"):
            self._cursor.rowcount = self._answers.get("rowcount", 0)

    def texts(self) -> list[str]:
        return [text for text, _params in self.statements]

    def matching(self, fragment: str) -> list[tuple[str, Any]]:
        return [(text, params) for text, params in self.statements if fragment in text]


# ── The mode ────────────────────────────────────────────────────────────────


class TestReloadMode:
    def test_unset_or_blank_is_the_shadow_default(self) -> None:
        assert DEFAULT_RELOAD_MODE is ReloadMode.SHADOW
        assert parse_reload_mode(None) is ReloadMode.SHADOW
        assert parse_reload_mode("  ") is ReloadMode.SHADOW

    @pytest.mark.parametrize(("raw", "mode"), [("touch", ReloadMode.TOUCH), (" Shadow ", ReloadMode.SHADOW), ("SEEN", ReloadMode.SEEN)])
    def test_each_mode_parses_case_and_space_insensitively(self, raw: str, mode: ReloadMode) -> None:
        assert parse_reload_mode(raw) is mode

    def test_anything_else_is_a_configuration_error(self) -> None:
        with pytest.raises(ValueError, match="RELOAD_MODE must be one of touch, shadow, seen"):
            parse_reload_mode("hash")

    def test_only_shadow_and_seen_append_and_only_seen_stops_touching(self) -> None:
        assert [mode.appends for mode in ReloadMode] == [False, True, True]
        assert [mode.touches for mode in ReloadMode] == [True, True, False]

    def test_the_configuration_reads_reload_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("RELOAD_MODE", raising=False)
        assert MusicBrainzSQLLoaderConfig.from_env().reload_mode is ReloadMode.SHADOW
        monkeypatch.setenv("RELOAD_MODE", "seen")
        assert MusicBrainzSQLLoaderConfig.from_env().reload_mode is ReloadMode.SEEN
        monkeypatch.setenv("RELOAD_MODE", "sometimes")
        with pytest.raises(ValueError, match="RELOAD_MODE"):
            MusicBrainzSQLLoaderConfig.from_env()


# ── The probe ───────────────────────────────────────────────────────────────


class TestProbe:
    @pytest.mark.asyncio
    async def test_the_probe_reports_every_missing_relation_and_function(self, mock_async_pool: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.fetchall.return_value = [("loader_seen.lists",), ("loader_seen.start(text, text)",)]

        assert await missing_seen_list_objects(mock_async_pool) == ["loader_seen.lists", "loader_seen.start(text, text)"]

        sql, params = mock_cursor.execute.await_args.args
        assert "to_regclass" in sql
        assert "to_regprocedure" in sql
        assert params["relations"] == ["loader_seen.lists", "loader_seen.musicbrainz_relationships", "loader_seen.musicbrainz_external_links"]
        for function in (
            "loader_seen.start(text, text)",
            "loader_seen.intact(text, text, timestamp with time zone)",
            "loader_seen.status(text, text, timestamp with time zone)",
            "loader_seen.hash_key(bytea)",
            "musicbrainz.external_link_key(uuid, text, text, text)",
            "loader_seen.musicbrainz_relationships_stale(timestamp with time zone)",
            "loader_seen.musicbrainz_relationships_shadow(timestamp with time zone, integer)",
            "loader_seen.musicbrainz_external_links_stale(timestamp with time zone)",
            "loader_seen.musicbrainz_external_links_shadow(timestamp with time zone, integer)",
        ):
            assert function in params["functions"]

    @pytest.mark.asyncio
    async def test_configured_touch_never_probes(self, mock_async_pool: MagicMock) -> None:
        assert await resolve_reload_mode(mock_async_pool, MagicMock(), ReloadMode.TOUCH) is ReloadMode.TOUCH
        mock_async_pool.connection.assert_not_called()

    @pytest.mark.parametrize("configured", [ReloadMode.SHADOW, ReloadMode.SEEN])
    @pytest.mark.asyncio
    async def test_a_schema_without_the_lists_runs_in_touch_mode(
        self, mock_async_pool: MagicMock, mock_cursor: MagicMock, configured: ReloadMode
    ) -> None:
        mock_cursor.fetchall.return_value = [("loader_seen.lists",)]
        logger = MagicMock()

        assert await resolve_reload_mode(mock_async_pool, logger, configured) is ReloadMode.TOUCH
        logger.warning.assert_called_once()
        assert logger.warning.call_args.kwargs["missing"] == ["loader_seen.lists"]

    @pytest.mark.asyncio
    async def test_a_failed_probe_runs_in_touch_mode(self, mock_async_pool: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.execute.side_effect = RuntimeError("connection reset")
        logger = MagicMock()

        assert await resolve_reload_mode(mock_async_pool, logger, ReloadMode.SEEN) is ReloadMode.TOUCH
        logger.error.assert_called_once()

    @pytest.mark.parametrize("configured", [ReloadMode.SHADOW, ReloadMode.SEEN])
    @pytest.mark.asyncio
    async def test_a_schema_with_the_lists_runs_the_configured_mode(
        self, mock_async_pool: MagicMock, mock_cursor: MagicMock, configured: ReloadMode
    ) -> None:
        mock_cursor.fetchall.return_value = []

        assert await resolve_reload_mode(mock_async_pool, MagicMock(), configured) is configured


# ── The writes ──────────────────────────────────────────────────────────────


class TestChildRowWrites:
    @pytest.mark.asyncio
    async def test_touch_mode_upserts_with_the_refresh_and_appends_nothing(self, mock_connection: MagicMock, mock_cursor: MagicMock) -> None:
        writer = PostgreSQLMusicBrainzWriter(refresh_updated_at=True)

        await writer.insert_relationships(mock_connection, [RELATIONSHIP])
        await writer.insert_external_links(mock_connection, [LINK])

        assert [call.args[0] for call in mock_cursor.executemany.await_args_list] == [RELATIONSHIP_UPSERTS[True], EXTERNAL_LINK_UPSERTS[True]]
        mock_cursor.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_shadow_mode_keeps_the_touch_and_appends_every_row(self, mock_connection: MagicMock, mock_cursor: MagicMock) -> None:
        writer = PostgreSQLMusicBrainzWriter(refresh_updated_at=True, reload_mode=ReloadMode.SHADOW)

        await writer.insert_relationships(mock_connection, [RELATIONSHIP])
        await writer.insert_external_links(mock_connection, [LINK])

        assert [call.args[0] for call in mock_cursor.executemany.await_args_list] == [RELATIONSHIP_UPSERTS[True], EXTERNAL_LINK_UPSERTS[True]]
        assert [call.args[0] for call in mock_cursor.execute.await_args_list] == [RELATIONSHIP_SEEN_APPEND, EXTERNAL_LINK_SEEN_APPEND]

    @pytest.mark.asyncio
    async def test_seen_mode_updates_only_a_changed_relationship_inserts_only_a_new_one_then_appends(
        self, mock_connection: MagicMock, mock_cursor: MagicMock
    ) -> None:
        writer = PostgreSQLMusicBrainzWriter(refresh_updated_at=True, reload_mode=ReloadMode.SEEN)

        await writer.insert_relationships(mock_connection, [RELATIONSHIP])

        update, insert = mock_cursor.executemany.await_args_list
        assert update.args[0] == RELATIONSHIP_SEEN_UPDATE
        (update_params,) = update.args[1]
        # `ended`, the eight key values in relationship_key's order, then `ended` again.
        assert update_params[:5] == (True, SOURCE, TARGET, "artist", "artist")
        assert update_params[5] == "member of band"
        assert update_params[6:8] == ("1990", None)
        assert isinstance(update_params[8], Jsonb)
        assert update_params[8].obj == ["guitar"]
        assert update_params[9] is True
        assert update_params.count(None) == 1
        assert insert.args[0] == RELATIONSHIP_SEEN_INSERT
        assert insert.args[1][0][:5] == RELATIONSHIP[:5]

        append_sql, append_params = mock_cursor.execute.await_args.args
        assert append_sql == RELATIONSHIP_SEEN_APPEND
        assert append_params[:7] == ([SOURCE], [TARGET], ["artist"], ["artist"], ["member of band"], ["1990"], [None])
        assert [attributes.obj for attributes in append_params[7]] == [["guitar"]]

    @pytest.mark.asyncio
    async def test_seen_mode_inserts_links_without_a_conflict_write_and_appends_in_key_order(
        self, mock_connection: MagicMock, mock_cursor: MagicMock
    ) -> None:
        writer = PostgreSQLMusicBrainzWriter(refresh_updated_at=True, reload_mode=ReloadMode.SEEN)

        await writer.insert_external_links(mock_connection, [LINK])

        assert mock_cursor.executemany.await_args.args == (EXTERNAL_LINK_SEEN_INSERT, [LINK])
        append_sql, append_params = mock_cursor.execute.await_args.args
        assert append_sql == EXTERNAL_LINK_SEEN_APPEND
        # (mbid, entity_type, service_name, url): external_link_key's order, not the insert's.
        assert append_params == ([SOURCE], ["artist"], ["official homepage"], ["https://example.com"])

    def test_the_seen_statements_write_nothing_on_an_unchanged_row(self) -> None:
        assert RELATIONSHIP_SEEN_INSERT.endswith("DO NOTHING")
        assert EXTERNAL_LINK_SEEN_INSERT.endswith("DO NOTHING")
        assert "AND ended IS DISTINCT FROM %s::boolean" in RELATIONSHIP_SEEN_UPDATE
        # The key is spelled exactly as the relationships_key index declares it, so the index serves the lookup.
        assert (
            "WHERE musicbrainz.relationship_key(source_mbid, target_mbid, source_entity_type, target_entity_type, "
            "relationship_type, begin_date, end_date, attributes) = musicbrainz.relationship_key(" in RELATIONSHIP_SEEN_UPDATE
        )
        assert "loader_seen.hash_key(musicbrainz.relationship_key(" in RELATIONSHIP_SEEN_APPEND
        assert "loader_seen.hash_key(musicbrainz.external_link_key(k.mbid, k.entity_type, k.service_name, k.url))" in EXTERNAL_LINK_SEEN_APPEND
        assert RELATIONSHIP_SEEN_APPEND.count("%s") == 8
        assert EXTERNAL_LINK_SEEN_APPEND.count("%s") == 4

    def test_the_dump_version_is_not_stored_in_the_entity_document(self) -> None:
        record = {"id": "x", "sha256": "abc", "dump_version": "20261101-001001", "name": "A", "relations": [], "external_links": []}

        assert _stored_data(record) == {"id": "x", "sha256": "abc", "name": "A"}


# ── The purges ──────────────────────────────────────────────────────────────


def _shadow_row(params: Any) -> tuple[Any, ...]:
    del params
    return (True, 100, 5, 5, 0, 0, [], [])


class TestShadowPurge:
    @pytest.mark.asyncio
    async def test_shadow_logs_both_comparisons_then_purges_by_updated_at_and_restarts_both_lists(
        self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock
    ) -> None:
        script = _ScriptedCursor(
            mock_cursor,
            mock_connection,
            {
                "_shadow": _shadow_row,
                "loader_seen.status": (BOUNDARY, True, 95),
                "WHERE updated_at <": (5,),
                "SELECT count(*) FROM": (100,),
                "rowcount": 5,
            },
        )
        logger = MagicMock()
        purge = _latched(mock_async_pool, ReloadMode.SHADOW, logger)

        deleted = await purge.purge()

        assert deleted == {"musicbrainz.relationships": 5, "musicbrainz.external_links": 5}
        texts = script.texts()
        shadows = [index for index, text in enumerate(texts) if "_shadow" in text]
        deletes = [index for index, text in enumerate(texts) if text.startswith("DELETE FROM")]
        assert len(shadows) == 2
        assert all(text.endswith("WHERE updated_at < %s") for text in (texts[index] for index in deletes))
        assert shadows[0] < deletes[0] and shadows[1] < deletes[1]
        assert '"loader_seen"."musicbrainz_relationships_shadow"' in texts[shadows[0]]
        assert '"loader_seen"."musicbrainz_external_links_shadow"' in texts[shadows[1]]
        assert [params for _text, params in script.matching("loader_seen.start")] == [
            ("musicbrainz", "relationships"),
            ("musicbrainz", "external_links"),
        ]
        assert not script.matching("_stale")
        comparisons = [call for call in logger.info.call_args_list if call.kwargs.get("seen_list")]
        assert [call.kwargs["matches"] for call in comparisons] == [True, True]
        assert comparisons[0].kwargs["touch_purge"] == comparisons[0].kwargs["seen_purge"] == 5
        assert comparisons[0].kwargs["status"]["seen_keys"] == 95

    @pytest.mark.asyncio
    async def test_a_differing_comparison_is_a_warning(self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock) -> None:
        _ScriptedCursor(
            mock_cursor,
            mock_connection,
            {"_shadow": (True, 100, 5, 7, 2, 0, [11, 12], []), "loader_seen.status": (BOUNDARY, True, 93), "SELECT count(*) FROM": (0,)},
        )
        logger = MagicMock()

        await _latched(mock_async_pool, ReloadMode.SHADOW, logger).purge()

        differing = [call for call in logger.warning.call_args_list if call.kwargs.get("seen_list")]
        assert len(differing) == 2
        assert differing[0].kwargs["refreshed_not_seen"] == 2
        assert differing[0].kwargs["refreshed_not_seen_sample"] == [11, 12]

    @pytest.mark.asyncio
    async def test_a_failed_comparison_never_blocks_the_touch_purge(
        self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock
    ) -> None:
        script = _ScriptedCursor(
            mock_cursor, mock_connection, {"WHERE updated_at <": (5,), "SELECT count(*) FROM": (100,), "rowcount": 5}, fail_on="_shadow"
        )
        logger = MagicMock()

        deleted = await _latched(mock_async_pool, ReloadMode.SHADOW, logger).purge()

        assert deleted == {"musicbrainz.relationships": 5, "musicbrainz.external_links": 5}
        assert len(script.matching("loader_seen.start")) == 2
        assert any("could not compare" in call.args[0] for call in logger.error.call_args_list)

    @pytest.mark.asyncio
    async def test_a_failed_restart_never_blocks_the_touch_purge(
        self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock
    ) -> None:
        _ScriptedCursor(
            mock_cursor,
            mock_connection,
            {"_shadow": _shadow_row, "WHERE updated_at <": (5,), "SELECT count(*) FROM": (100,), "rowcount": 5},
            fail_on="loader_seen.start",
        )
        logger = MagicMock()
        purge = _latched(mock_async_pool, ReloadMode.SHADOW, logger)

        assert await purge.purge() == {"musicbrainz.relationships": 5, "musicbrainz.external_links": 5}
        assert purge.is_settled
        assert any("could not restart the seen lists" in call.args[0] for call in logger.error.call_args_list)


class TestSeenPurge:
    @pytest.mark.asyncio
    async def test_seen_purges_each_table_from_its_stale_set_by_id_then_restarts_both_lists(
        self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock
    ) -> None:
        script = _ScriptedCursor(
            mock_cursor,
            mock_connection,
            {
                "loader_seen.intact": (True,),
                "loader_seen.status": (BOUNDARY, True, 95),
                'SELECT count(*) FROM "stale_': (5,),
                "SELECT count(*) FROM": (100,),
                "rowcount": 5,
            },
        )
        purge = _latched(mock_async_pool, ReloadMode.SEEN)

        deleted = await purge.purge()

        assert deleted == {"musicbrainz.relationships": 5, "musicbrainz.external_links": 5}
        texts = script.texts()
        assert [params for _text, params in script.matching("loader_seen.intact")] == [
            ("musicbrainz", "relationships", BOUNDARY),
            ("musicbrainz", "external_links", BOUNDARY),
        ]
        stale = script.matching("CREATE TEMP TABLE")
        assert [text for text, _params in stale] == [
            'CREATE TEMP TABLE "stale_relationships" ON COMMIT DROP AS SELECT id FROM "loader_seen"."musicbrainz_relationships_stale"(%s)',
            'CREATE TEMP TABLE "stale_external_links" ON COMMIT DROP AS SELECT id FROM "loader_seen"."musicbrainz_external_links_stale"(%s)',
        ]
        assert [params for _text, params in stale] == [(BOUNDARY,), (BOUNDARY,)]
        assert [text for text in texts if text.startswith("DELETE FROM")] == [
            'DELETE FROM "musicbrainz"."relationships" WHERE id IN (SELECT id FROM "stale_relationships")',
            'DELETE FROM "musicbrainz"."external_links" WHERE id IN (SELECT id FROM "stale_external_links")',
        ]
        assert not any("updated_at <" in text for text in texts)
        assert texts[-2:] == ["SELECT loader_seen.start(%s, %s)", "SELECT loader_seen.start(%s, %s)"]
        assert purge.lost_lists == ()
        assert purge.is_settled

    @pytest.mark.asyncio
    async def test_a_list_that_is_not_intact_purges_nothing_restarts_both_and_reports_it(
        self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock
    ) -> None:
        script = _ScriptedCursor(
            mock_cursor,
            mock_connection,
            {"loader_seen.intact": lambda params: (params[1] != "external_links",)},
        )
        logger = MagicMock()
        purge = _latched(mock_async_pool, ReloadMode.SEEN, logger)

        assert await purge.purge() == {}
        assert purge.lost_lists == ("loader_seen.musicbrainz_external_links",)
        assert not [text for text in script.texts() if text.startswith(("DELETE", "CREATE TEMP", "SELECT count"))]
        assert len(script.matching("loader_seen.start")) == 2
        assert purge.is_settled
        logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_the_delete_fraction_cap_refuses_a_stale_set_that_large(
        self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock
    ) -> None:
        script = _ScriptedCursor(
            mock_cursor,
            mock_connection,
            {"loader_seen.intact": (True,), 'SELECT count(*) FROM "stale_': (95,), "SELECT count(*) FROM": (100,)},
        )

        assert await _latched(mock_async_pool, ReloadMode.SEEN).purge() == {"musicbrainz.relationships": 0, "musicbrainz.external_links": 0}
        assert not [text for text in script.texts() if text.startswith("DELETE")]
        assert len(script.matching("loader_seen.start")) == 2

    @pytest.mark.asyncio
    async def test_an_empty_table_or_an_empty_stale_set_deletes_nothing(
        self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock
    ) -> None:
        totals = iter([(0,), (100,)])
        script = _ScriptedCursor(
            mock_cursor,
            mock_connection,
            {"loader_seen.intact": (True,), 'SELECT count(*) FROM "stale_': (0,), "SELECT count(*) FROM": lambda _params: next(totals)},
        )

        assert await _latched(mock_async_pool, ReloadMode.SEEN).purge() == {"musicbrainz.relationships": 0, "musicbrainz.external_links": 0}
        assert len(script.matching("CREATE TEMP TABLE")) == 1
        assert not [text for text in script.texts() if text.startswith("DELETE")]

    @pytest.mark.asyncio
    async def test_a_failed_restart_rolls_the_seen_purge_back_and_requeues(
        self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock
    ) -> None:
        _ScriptedCursor(
            mock_cursor,
            mock_connection,
            {"loader_seen.intact": (True,), 'SELECT count(*) FROM "stale_': (5,), "SELECT count(*) FROM": (100,), "rowcount": 5},
            fail_on="loader_seen.start",
        )
        purge = _latched(mock_async_pool, ReloadMode.SEEN)

        with pytest.raises(RuntimeError, match=r"loader_seen\.start"):
            await purge.purge()
        assert not purge.is_settled


class TestVetoedExtractionsRestartTheirLists:
    @pytest.mark.parametrize("mode", [ReloadMode.SHADOW, ReloadMode.SEEN])
    @pytest.mark.asyncio
    async def test_a_dead_letter_veto_restarts_both_lists_in_a_transaction_of_their_own(
        self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock, mode: ReloadMode
    ) -> None:
        script = _ScriptedCursor(mock_cursor, mock_connection, {})
        purge = _latched(mock_async_pool, mode)
        purge.record_dead_letter("releases")

        assert await purge.purge() == {}
        assert script.texts() == ["SELECT loader_seen.start(%s, %s)", "SELECT loader_seen.start(%s, %s)"]
        assert purge.is_settled

        # A duplicate signal for the settled extraction must not empty lists the next one may be filling.
        await purge.purge()
        assert len(script.texts()) == 2

    @pytest.mark.asyncio
    async def test_touch_mode_never_touches_a_list(self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock) -> None:
        script = _ScriptedCursor(mock_cursor, mock_connection, {})
        purge = _latched(mock_async_pool, ReloadMode.TOUCH)
        purge.record_dead_letter("releases")

        await purge.purge()
        assert script.texts() == []

    @pytest.mark.asyncio
    async def test_an_unlatched_purge_restarts_nothing(self, mock_async_pool: MagicMock, mock_connection: MagicMock, mock_cursor: MagicMock) -> None:
        script = _ScriptedCursor(mock_cursor, mock_connection, {})
        purge = StaleChildRowPurge(mock_async_pool, MagicMock(), admission=WriteAdmission(4), mode=ReloadMode.SEEN)
        purge.record_completion("artists", _signal())

        await purge.purge()
        assert script.texts() == []


class TestTheServiceRecordsALostList:
    @pytest.mark.asyncio
    async def test_a_lost_list_is_recorded_as_a_veto_for_the_extraction(self, monkeypatch: pytest.MonkeyPatch, mock_async_pool: MagicMock) -> None:
        record = AsyncMock()
        monkeypatch.setattr(service, "record_extraction", record)
        monkeypatch.setattr(service, "connection_pool", mock_async_pool)
        purge = MagicMock(lost_lists=("loader_seen.musicbrainz_relationships",))

        await service._record_lost_seen_lists(purge, _signal())

        record.assert_awaited_once_with(mock_async_pool, "2026-11-01", ["loader_seen.musicbrainz_relationships"])

    @pytest.mark.asyncio
    async def test_nothing_is_recorded_when_no_list_was_lost(self, monkeypatch: pytest.MonkeyPatch, mock_async_pool: MagicMock) -> None:
        record = AsyncMock()
        monkeypatch.setattr(service, "record_extraction", record)
        monkeypatch.setattr(service, "connection_pool", mock_async_pool)

        await service._record_lost_seen_lists(MagicMock(lost_lists=()), _signal())

        record.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_missing_admission_raises_so_the_signal_requeues(self, monkeypatch: pytest.MonkeyPatch, mock_async_pool: MagicMock) -> None:
        monkeypatch.setattr(service, "connection_pool", mock_async_pool)
        monkeypatch.setattr(service, "write_admission", None)

        with pytest.raises(Exception, match="Write admission"):
            await service._record_lost_seen_lists(MagicMock(lost_lists=("loader_seen.musicbrainz_relationships",)), _signal())


def test_a_purge_built_without_a_mode_is_a_touch_purge(mock_async_pool: MagicMock) -> None:
    assert StaleChildRowPurge(mock_async_pool, MagicMock()).mode is ReloadMode.TOUCH
