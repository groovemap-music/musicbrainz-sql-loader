"""Tests for the private MusicBrainz record-processing boundary."""

from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from brainztableinator._record_processing import MusicBrainzRecordProcessor


ROOT = Path(__file__).parent.parent


class _Observer:
    def __init__(self) -> None:
        self.records: list[tuple[str, int, str]] = []

    @contextmanager
    def flush(self, _entity: str) -> Any:
        yield MagicMock()

    def record(self, entity: str, size: int, _duration_s: float, outcome: str) -> None:
        self.records.append((entity, size, outcome))

    def set_outcome(self, span: Any, outcome: str) -> None:
        span.set_attribute("outcome", outcome)


def _writer() -> MagicMock:
    writer = MagicMock()
    for operation in (
        "upsert_artist",
        "upsert_label",
        "upsert_release",
        "upsert_release_group",
        "insert_relationships",
        "insert_external_links",
    ):
        setattr(writer, operation, AsyncMock())
    return writer


@pytest.mark.asyncio
async def test_record_processor_maps_an_artist_before_delegating_writes() -> None:
    writer = _writer()
    observer = _Observer()
    processor = MusicBrainzRecordProcessor(writer, observer, MagicMock())
    conn = MagicMock()
    record = {
        "id": "artist-id",
        "name": "Artist",
        "life_span": {"ended": None},
        "aliases": None,
        "relations": [{"target_mbid": "label-id", "target_type": "label", "type": "contract"}],
        "external_links": [{"url": "https://example.com", "service": "homepage"}],
    }

    await processor.process_artist(conn, record)

    values = writer.upsert_artist.await_args.args[1]
    assert values[0] == "artist-id"
    assert values[7] is False
    assert values[13] == []
    assert values[15] is record
    writer.insert_relationships.assert_awaited_once()
    writer.insert_external_links.assert_awaited_once()
    assert [record[:2] for record in observer.records] == [("artist", 1), ("artist", 1)]


def test_runtime_module_contains_no_entity_sql() -> None:
    runtime = (ROOT / "brainztableinator/brainztableinator.py").read_text(encoding="utf-8")
    persistence = (ROOT / "brainztableinator/_persistence.py").read_text(encoding="utf-8")

    assert "INSERT INTO musicbrainz" not in runtime
    for table in ("artists", "labels", "releases", "release_groups", "relationships", "external_links"):
        assert f"INSERT INTO musicbrainz.{table}" in persistence
