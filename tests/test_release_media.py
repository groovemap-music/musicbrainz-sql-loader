"""Tests for the MusicBrainz half of graph.issued_on and the shared media vocabulary.

Two things are pinned here. The derivation — which medium a release is issued on, and in what
quantity — must agree with brainzgraphinator, the MusicBrainz graph enricher, because both
project the same canonical media block onto the same graph and a disagreement shows up as a
parity failure rather than as an error. And the statements must leave every row
`discogs-sql-loader` owns alone, which is what the `source` column in the primary key of
`graph.issued_on` is for.
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from common.media import map_musicbrainz_release

from brainztableinator._persistence import PostgreSQLMusicBrainzWriter
from brainztableinator._record_processing import MEDIA_SOURCE, MusicBrainzRecordProcessor, media_edge_rows


ROOT = Path(__file__).parent.parent


class _Observer:
    @contextmanager
    def flush(self, _entity: str) -> Any:
        yield MagicMock()

    def record(self, _entity: str, _size: int, _duration_s: float, _outcome: str) -> None:
        return None

    def set_outcome(self, _span: Any, _outcome: str) -> None:
        return None


def _writer() -> MagicMock:
    """Return a writer double whose every operation is awaitable."""
    writer = MagicMock()
    for operation in (
        "upsert_artist",
        "upsert_label",
        "upsert_release",
        "upsert_release_group",
        "insert_relationships",
        "insert_external_links",
        "upsert_media_families",
        "upsert_media",
        "replace_release_media_edges",
    ):
        setattr(writer, operation, AsyncMock())
    return writer


def _processor(writer: MagicMock) -> MusicBrainzRecordProcessor:
    return MusicBrainzRecordProcessor(writer, _Observer(), map_musicbrainz_release)


def _block(*items: dict[str, Any]) -> dict[str, Any]:
    """Return a minimal canonical media block holding ITEMS."""
    return {"items": list(items)}


def _item(medium: str | None, family: str | None, qty: Any = 1) -> dict[str, Any]:
    return {"medium": medium, "family": family, "qty": qty}


# ── The derivation ───────────────────────────────────────────────────────────


def test_a_release_projects_one_row_per_canonical_medium() -> None:
    media = map_musicbrainz_release(
        {"media": [{"format": '12" Vinyl', "position": 1}, {"format": "CD", "position": 2}], "status": "Official"},
    )

    assert media_edge_rows(media) == [
        {"medium": "optical_cd", "family": "optical", "label": "CD", "qty": 1},
        {"medium": "vinyl_12", "family": "vinyl", "label": '12" vinyl', "qty": 1},
    ]


def test_two_entries_resolving_to_one_medium_become_one_row_whose_quantity_is_their_sum() -> None:
    # A 2xLP the provider states as two separate media entries. brainzgraphinator collapses
    # these into one edge, and the schema's `issued_on` body groups and sums for the same
    # reason; a pair of rows here would violate the primary key the third one writes under.
    media = map_musicbrainz_release({"media": [{"format": '12" Vinyl', "position": 1}, {"format": '12" Vinyl', "position": 2}]})

    assert media_edge_rows(media) == [{"medium": "vinyl_12", "family": "vinyl", "label": '12" vinyl', "qty": 2}]


def test_rows_come_back_ordered_by_medium_id() -> None:
    rows = media_edge_rows(_block(_item("vinyl_12", "vinyl"), _item("digital_file", "digital"), _item("optical_cd", "optical")))

    assert [row["medium"] for row in rows] == ["digital_file", "optical_cd", "vinyl_12"]


@pytest.mark.parametrize(
    "quantity",
    [None, 0, -3, True, False, 2.0, "2", [2], 1_000_000_000],
)
def test_a_quantity_that_is_not_a_whole_number_of_at_least_one_counts_as_one_unit(quantity: Any) -> None:
    # brainzgraphinator keeps a non-boolean `int` of at least one and defaults everything else,
    # and `_MEDIUM_QUANTITY` in the schema bounds the value it casts to nine digits so a
    # pathological quantity defaults rather than overflowing the `bigint` column both write.
    assert media_edge_rows(_block(_item("optical_cd", "optical", quantity)))[0]["qty"] == 1


def test_a_whole_quantity_within_the_bound_is_kept_verbatim() -> None:
    assert media_edge_rows(_block(_item("optical_cd", "optical", 999_999_999)))[0]["qty"] == 999_999_999


@pytest.mark.parametrize(
    "item",
    [
        "not a mapping",
        None,
        {"medium": "optical_cd"},
        {"family": "optical"},
        {"medium": None, "family": "optical"},
        {"medium": "optical_cd", "family": None},
        {"medium": "", "family": "optical"},
        {"medium": "optical_cd", "family": ""},
        {"medium": 7, "family": "optical"},
        {"medium": "optical_cd", "family": 7},
    ],
)
def test_an_entry_without_a_usable_medium_and_family_contributes_no_row(item: Any) -> None:
    # The enricher drops an entry whose medium or family is not a string; the schema's
    # `_MEDIA_SOURCE` drops one whose text is empty. Requiring both holds either rule.
    assert media_edge_rows(_block(item)) == []


def test_a_medium_the_vendored_vocabulary_does_not_hold_falls_back_to_its_own_id() -> None:
    # `graph.medium_label` ends its CASE in `ELSE medium_id` and the enricher catches the
    # KeyError, so a block written under a newer taxonomy still yields a labelled row.
    assert media_edge_rows(_block(_item("holographic_cube", "future")))[0]["label"] == "holographic_cube"


def test_a_media_block_holding_nothing_yields_no_rows() -> None:
    assert media_edge_rows({}) == []
    assert media_edge_rows({"items": None}) == []
    assert media_edge_rows(_block()) == []


# ── The release key ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_release_naming_no_discogs_id_writes_no_media_row() -> None:
    # The graph keys a release on its Discogs id, so there is no key to write under. This is
    # what `enrich_release` in brainzgraphinator does with the same record: it counts it under
    # `entities_skipped_no_discogs_match` and reconciles no media.
    writer = _writer()

    written = await _processor(writer).write_release_media(MagicMock(), {"mbid": "release-mbid"}, _block(_item("optical_cd", "optical")))

    assert written is False
    writer.replace_release_media_edges.assert_not_awaited()
    writer.upsert_media.assert_not_awaited()
    writer.upsert_media_families.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_release_key_is_the_discogs_id_rendered_as_the_text_the_graph_keys_on() -> None:
    # `graph.release.release_id` is `public.releases.data_id` as text, and the schema reaches
    # the MusicBrainz side through `musicbrainz.releases.discogs_release_id`, so an id the
    # producer sent as a number must be written as its digits.
    writer = _writer()

    await _processor(writer).write_release_media(MagicMock(), {"discogs_release_id": 4242}, _block(_item("optical_cd", "optical")))

    _conn, release_id, source, rows = writer.replace_release_media_edges.await_args.args
    assert (release_id, source) == ("4242", "musicbrainz")
    assert rows == [("4242", "optical_cd", "musicbrainz", 1)]


@pytest.mark.asyncio
async def test_the_source_this_loader_writes_is_musicbrainz() -> None:
    assert MEDIA_SOURCE == "musicbrainz"

    writer = _writer()
    await _processor(writer).write_release_media(MagicMock(), {"discogs_release_id": "1"}, _block(_item("optical_cd", "optical")))

    assert writer.replace_release_media_edges.await_args.args[2] == MEDIA_SOURCE


@pytest.mark.asyncio
async def test_a_release_whose_media_yields_nothing_still_prunes_its_own_rows() -> None:
    # The enricher runs its prune with an empty `$medium_ids`, deleting every edge it had
    # written for the release. Keeping a stale row would leave `graph.issued_on` describing
    # media the document this loader just stored no longer names.
    writer = _writer()

    await _processor(writer).write_release_media(MagicMock(), {"discogs_release_id": 7}, _block())

    assert writer.replace_release_media_edges.await_args.args[1:] == ("7", "musicbrainz", [])
    assert writer.upsert_media.await_args.args[1] == []
    assert writer.upsert_media_families.await_args.args[1] == []


@pytest.mark.asyncio
async def test_the_vocabulary_upserts_carry_the_families_and_media_the_rows_name() -> None:
    writer = _writer()
    media = map_musicbrainz_release({"media": [{"format": '12" Vinyl'}, {"format": "CD"}, {"format": "CD"}]})

    await _processor(writer).write_release_media(MagicMock(), {"discogs_release_id": 9}, media)

    assert writer.upsert_media_families.await_args.args[1] == ["optical", "vinyl"]
    assert writer.upsert_media.await_args.args[1] == [("optical_cd", "optical", "CD"), ("vinyl_12", "vinyl", '12" vinyl')]


# ── The release message ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_processing_a_release_writes_its_media_on_the_connection_the_document_is_written_on() -> None:
    # The caller wraps one `process_release` in one transaction, so writing the edges on the
    # same connection is what makes them commit or roll back with the release row.
    writer = _writer()
    conn = MagicMock()
    record = {"mbid": "release-mbid", "discogs_release_id": 4242, "media": {"items": [{"medium": "optical_cd", "family": "optical", "qty": 2}]}}

    await _processor(writer).process_release(conn, record)

    assert writer.upsert_release.await_args.args[0] is conn
    assert writer.replace_release_media_edges.await_args.args[0] is conn
    assert writer.replace_release_media_edges.await_args.args[3] == [("4242", "optical_cd", "musicbrainz", 2)]


@pytest.mark.asyncio
async def test_the_stored_media_block_and_the_media_rows_describe_the_same_media() -> None:
    # A legacy event carries no canonical block, so the loader derives one, stores it on the
    # release row, and must project the rows out of that same block rather than a second
    # derivation that could drift from it.
    writer = _writer()
    record = {
        "mbid": "release-mbid",
        "discogs_release_id": 4242,
        "media_raw": [{"format": '12" Vinyl'}, {"format": '12" Vinyl'}],
        "status": "Official",
    }

    await _processor(writer).process_release(MagicMock(), record)

    stored = writer.upsert_release.await_args.args[1][6]
    assert [item["medium"] for item in stored["items"]] == ["vinyl_12", "vinyl_12"]
    assert writer.replace_release_media_edges.await_args.args[3] == [("4242", "vinyl_12", "musicbrainz", 2)]


# ── The statements ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_prune_names_both_the_release_and_the_source(mock_connection: Any, mock_cursor: Any) -> None:
    # `source` is in the primary key of `graph.issued_on` so that this DELETE cannot reach a
    # row `discogs-sql-loader` wrote. A prune that named only the release would delete them.
    await PostgreSQLMusicBrainzWriter().replace_release_media_edges(mock_connection, "4242", "musicbrainz", [])

    sql, params = mock_cursor.execute.await_args.args
    assert sql == "DELETE FROM graph.issued_on WHERE release_id = %s AND source = %s"
    assert params == ("4242", "musicbrainz")
    mock_cursor.executemany.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_edges_are_inserted_after_the_prune_with_no_conflict_clause(mock_connection: Any, mock_cursor: Any) -> None:
    rows = [("4242", "optical_cd", "musicbrainz", 2)]

    await PostgreSQLMusicBrainzWriter().replace_release_media_edges(mock_connection, "4242", "musicbrainz", rows)

    sql, params = mock_cursor.executemany.await_args.args
    assert sql == "INSERT INTO graph.issued_on (release_id, medium_id, source, qty) VALUES (%s, %s, %s, %s)"
    assert params == rows
    assert "ON CONFLICT" not in sql


@pytest.mark.asyncio
async def test_the_shared_media_vocabulary_never_overwrites_the_row_that_is_already_there(mock_connection: Any, mock_cursor: Any) -> None:
    # `graph.medium` and `graph.media_family` are written by both SQL loaders. The enricher
    # sets a medium's family and label under ON CREATE only, so neither loader may overwrite
    # what the other wrote.
    writer = PostgreSQLMusicBrainzWriter()

    await writer.upsert_media_families(mock_connection, ["optical"])
    families_sql, families_params = mock_cursor.executemany.await_args.args
    await writer.upsert_media(mock_connection, [("optical_cd", "optical", "CD")])
    media_sql, media_params = mock_cursor.executemany.await_args.args

    assert families_sql == "INSERT INTO graph.media_family (name) VALUES (%s) ON CONFLICT DO NOTHING"
    assert families_params == [("optical",)]
    assert media_sql == "INSERT INTO graph.medium (medium_id, family, label) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
    assert media_params == [("optical_cd", "optical", "CD")]
    assert "DO UPDATE" not in families_sql
    assert "DO UPDATE" not in media_sql


@pytest.mark.asyncio
async def test_an_empty_vocabulary_write_runs_no_statement(mock_connection: Any, mock_cursor: Any) -> None:
    writer = PostgreSQLMusicBrainzWriter()

    await writer.upsert_media_families(mock_connection, [])
    await writer.upsert_media(mock_connection, [])

    mock_cursor.executemany.assert_not_awaited()


def test_the_graph_statements_live_in_the_persistence_module() -> None:
    # The same boundary `test_runtime_module_contains_no_entity_sql` holds for the MusicBrainz
    # tables: the runtime module coordinates and the persistence module spells the SQL.
    runtime = (ROOT / "brainztableinator/brainztableinator.py").read_text(encoding="utf-8")
    persistence = (ROOT / "brainztableinator/_persistence.py").read_text(encoding="utf-8")
    record_processing = (ROOT / "brainztableinator/_record_processing.py").read_text(encoding="utf-8")

    for relation in ("graph.issued_on", "graph.medium", "graph.media_family"):
        assert f"INTO {relation}" in persistence or f"FROM {relation}" in persistence
        assert relation not in runtime
        assert f"INSERT INTO {relation}" not in record_processing
