"""Coverage for the native catalog id each MusicBrainz upsert carries.

The four statements are the only place ``gm_item_id`` reaches PostgreSQL, so these tests assert
the column is both inserted and refreshed on conflict, and that it lands in the parameter
position the placeholder list gives it. A writer that dropped the column from one of the four
would otherwise keep every other test passing and silently leave that table's rows unlinked.
"""

import re
from typing import Any
from uuid import UUID, uuid4

import pytest

from brainztableinator._persistence import PostgreSQLMusicBrainzWriter


# Each entry is the writer method, the table it targets, and a values tuple whose last element
# is the native id. The tuples mirror what the record processor builds, padded with placeholders
# for the fields these tests do not exercise.
UPSERTS = (
    ("upsert_artist", "musicbrainz.artists", 16),
    ("upsert_label", "musicbrainz.labels", 11),
    ("upsert_release", "musicbrainz.releases", 8),
    ("upsert_release_group", "musicbrainz.release_groups", 8),
)

# The JSON-bearing positions per writer, so the padding below is a dict where the statement
# expects one and a plain string everywhere else.
JSON_POSITIONS = {
    "upsert_artist": (13, 14, 15),
    "upsert_label": (10,),
    "upsert_release": (6, 7),
    "upsert_release_group": (3, 7),
}


def _values(method: str, native_id: UUID | None, width: int) -> tuple[Any, ...]:
    fields: list[Any] = [{} if position in JSON_POSITIONS[method] else "x" for position in range(width)]
    return (*fields, native_id)


@pytest.mark.parametrize(("method", "table", "native_id_position"), UPSERTS)
@pytest.mark.asyncio
async def test_every_upsert_inserts_and_refreshes_the_native_id(
    mock_connection: Any,
    mock_cursor: Any,
    method: str,
    table: str,
    native_id_position: int,
) -> None:
    native_id = uuid4()
    await getattr(PostgreSQLMusicBrainzWriter(), method)(mock_connection, _values(method, native_id, native_id_position))

    sql, params = mock_cursor.execute.await_args.args
    columns = re.search(rf"INSERT INTO {re.escape(table)} \(([^)]*)\)", sql).group(1)
    placeholders = re.search(r"VALUES \(([^)]*)\)", sql).group(1)

    assert [column.strip() for column in columns.split(",")][-1] == "gm_item_id"
    assert len(placeholders.split(",")) == len(columns.split(","))
    assert "gm_item_id = EXCLUDED.gm_item_id" in sql
    assert len(params) == len(columns.split(","))
    assert params[native_id_position] == native_id


@pytest.mark.parametrize(("method", "native_id_position"), [(method, position) for method, _table, position in UPSERTS])
@pytest.mark.asyncio
async def test_an_unresolved_record_still_upserts_with_a_null_native_id(
    mock_connection: Any,
    mock_cursor: Any,
    method: str,
    native_id_position: int,
) -> None:
    # Identity is additive: a row whose alias could not be resolved is still written, with the
    # column left NULL for the reconciliation job to fill rather than the message dead-lettered.
    await getattr(PostgreSQLMusicBrainzWriter(), method)(mock_connection, _values(method, None, native_id_position))

    assert mock_cursor.execute.await_args.args[1][native_id_position] is None
