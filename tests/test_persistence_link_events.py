"""Statement-shape coverage for the link-change event each MusicBrainz upsert records.

The behaviour (which change writes which event, and that the event shares the upsert's fate) is
asserted against real PostgreSQL in ``tests/integration/test_link_events.py``. These tests need
no database: they pin what a cursor double can see, that each statement is one round trip whose
placeholders line up with its parameters, and that the source reference reaches the writer.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from brainztableinator._persistence import PostgreSQLMusicBrainzWriter
from brainztableinator._record_processing import UNVERSIONED_SOURCE_REF, MusicBrainzRecordProcessor, _source_ref
from tests.test_persistence_native_id import UPSERTS, _values


# The entity kind and Discogs column each writer names, per `provider_aliases`' vocabulary.
EVENT_TARGETS = {
    "upsert_artist": ("artist", "discogs_artist_id"),
    "upsert_label": ("label", "discogs_label_id"),
    "upsert_release": ("release", "discogs_release_id"),
    "upsert_release_group": ("master", "discogs_master_id"),
}


@pytest.mark.parametrize(("method", "native_id_position"), [(method, position) for method, _table, position in UPSERTS])
@pytest.mark.asyncio
async def test_each_upsert_is_one_statement_whose_placeholders_match_its_parameters(
    mock_connection: Any,
    mock_cursor: Any,
    method: str,
    native_id_position: int,
) -> None:
    values = _values(method, uuid4(), native_id_position)
    await getattr(PostgreSQLMusicBrainzWriter(), method)(mock_connection, values, "20261021-001001")

    mock_cursor.execute.assert_awaited_once()
    sql, params = mock_cursor.execute.await_args.args
    assert sql.count("%s") == len(params)
    # The lookup's MBID, then the source reference, close the parameters.
    assert params[-2:] == (values[0], "20261021-001001")


@pytest.mark.parametrize(("method", "target"), EVENT_TARGETS.items())
@pytest.mark.asyncio
async def test_each_upsert_records_its_own_entity_kind_and_discogs_column(
    mock_connection: Any,
    mock_cursor: Any,
    method: str,
    target: tuple[str, str],
) -> None:
    entity_kind, column = target
    position = next(position for name, _table, position in UPSERTS if name == method)
    await getattr(PostgreSQLMusicBrainzWriter(), method)(mock_connection, _values(method, None, position), "20261021-001001")

    sql = mock_cursor.execute.await_args.args[0]
    assert f"SELECT 'musicbrainz', '{entity_kind}'," in sql
    assert f"RETURNING mbid, {column}" in sql
    assert f"old.{column} IS DISTINCT FROM upsert.{column}" in sql
    assert "'groovemap_load'" in sql
    assert "ON CONFLICT ON CONSTRAINT identity_link_events_idempotence_key DO NOTHING" in sql


def test_a_dump_version_on_the_record_is_the_source_reference() -> None:
    assert _source_ref({"dump_version": "20261021-001001"}) == "20261021-001001"


@pytest.mark.parametrize("record", [{}, {"dump_version": None}, {"dump_version": ""}, {"dump_version": 20261021}])
def test_a_record_without_a_usable_dump_version_is_filed_as_unversioned(record: dict[str, Any]) -> None:
    assert _source_ref(record) == UNVERSIONED_SOURCE_REF


@pytest.mark.asyncio
async def test_the_processor_hands_the_record_s_source_reference_to_the_writer(mock_connection: Any) -> None:
    writer = MagicMock()
    for name in ("upsert_artist", "insert_relationships", "insert_external_links"):
        setattr(writer, name, AsyncMock())
    observer = MagicMock()
    processor = MusicBrainzRecordProcessor(writer, observer, MagicMock())

    await processor.process_artist(mock_connection, {"mbid": str(uuid4()), "name": "A", "dump_version": "20261021-001001"})

    assert writer.upsert_artist.await_args.args[2] == "20261021-001001"
