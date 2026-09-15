"""Tests for the private MusicBrainz record-processing boundary."""

from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
import structlog
from common.identity import AliasRef

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


def _identity(
    monkeypatch: pytest.MonkeyPatch,
    *,
    resolved: dict[AliasRef, UUID] | None = None,
    attached: dict[AliasRef, UUID] | None = None,
) -> tuple[AsyncMock, AsyncMock]:
    """Replace the two identity calls with recorders that answer from fixed tables.

    A ref absent from ``resolved`` is an alias no provider has claimed, which is what an
    unresolvable Discogs id looks like from the loader's side.
    """
    resolve = AsyncMock(side_effect=lambda _conn, refs: {ref: (resolved or {})[ref] for ref in refs if ref in (resolved or {})})
    attach = AsyncMock(side_effect=lambda _conn, mapping: {ref: (attached or mapping)[ref] for ref in mapping})
    monkeypatch.setattr("brainztableinator._record_processing.resolve_aliases", resolve)
    monkeypatch.setattr("brainztableinator._record_processing.attach_aliases", attach)
    return resolve, attach


@pytest.mark.asyncio
async def test_a_release_naming_a_discogs_id_attaches_to_that_item_instead_of_minting(monkeypatch: pytest.MonkeyPatch) -> None:
    discogs_ref = AliasRef("discogs", "release", "4242")
    musicbrainz_ref = AliasRef("musicbrainz", "release", "release-mbid")
    native_id = uuid4()
    resolve, attach = _identity(monkeypatch, resolved={discogs_ref: native_id})
    writer = _writer()
    processor = MusicBrainzRecordProcessor(writer, _Observer(), MagicMock())
    conn = MagicMock()

    await processor.process_release(conn, {"mbid": "release-mbid", "discogs_release_id": 4242, "media": {}})

    # The Discogs alias is read first and the MusicBrainz alias is attached to what it named,
    # so no second catalog item is minted for a record the Discogs loader already owns.
    resolve.assert_awaited_once_with(conn, [discogs_ref])
    attach.assert_awaited_once_with(conn, {musicbrainz_ref: native_id})
    assert writer.upsert_release.await_args.args[1][8] == native_id


@pytest.mark.asyncio
async def test_an_attach_losing_to_an_existing_alias_writes_the_id_that_alias_already_names(monkeypatch: pytest.MonkeyPatch) -> None:
    discogs_ref = AliasRef("discogs", "artist", "77")
    musicbrainz_ref = AliasRef("musicbrainz", "artist", "artist-mbid")
    discogs_native_id, incumbent_id = uuid4(), uuid4()
    _identity(monkeypatch, resolved={discogs_ref: discogs_native_id}, attached={musicbrainz_ref: incumbent_id})
    writer = _writer()
    processor = MusicBrainzRecordProcessor(writer, _Observer(), MagicMock())

    await processor.process_artist(MagicMock(), {"mbid": "artist-mbid", "discogs_artist_id": 77})

    # attach_aliases never overwrites: the row keeps whatever the existing alias resolves to.
    assert writer.upsert_artist.await_args.args[1][16] == incumbent_id


@pytest.mark.asyncio
async def test_a_label_without_a_discogs_id_mints_through_its_own_musicbrainz_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    musicbrainz_ref = AliasRef("musicbrainz", "label", "label-mbid")
    native_id = uuid4()
    resolve, attach = _identity(monkeypatch, resolved={musicbrainz_ref: native_id})
    writer = _writer()
    processor = MusicBrainzRecordProcessor(writer, _Observer(), MagicMock())
    conn = MagicMock()

    await processor.process_label(conn, {"mbid": "label-mbid"})

    resolve.assert_awaited_once_with(conn, [musicbrainz_ref])
    attach.assert_not_awaited()
    assert writer.upsert_label.await_args.args[1][11] == native_id


@pytest.mark.asyncio
async def test_a_discogs_id_no_alias_claims_falls_back_to_minting_through_musicbrainz(monkeypatch: pytest.MonkeyPatch) -> None:
    discogs_ref = AliasRef("discogs", "master", "999")
    musicbrainz_ref = AliasRef("musicbrainz", "master", "release-group-mbid")
    native_id = uuid4()
    resolve, attach = _identity(monkeypatch, resolved={musicbrainz_ref: native_id})
    writer = _writer()
    processor = MusicBrainzRecordProcessor(writer, _Observer(), MagicMock())
    conn = MagicMock()

    await processor.process_release_group(conn, {"mbid": "release-group-mbid", "discogs_master_id": 999})

    # A release group resolves as the master kind, and a Discogs id no alias claims yet mints
    # rather than blocking the row; reconciling the pair later is the reconciliation job's work.
    assert [call.args for call in resolve.await_args_list] == [(conn, [discogs_ref]), (conn, [musicbrainz_ref])]
    attach.assert_not_awaited()
    assert writer.upsert_release_group.await_args.args[1][8] == native_id


@pytest.mark.asyncio
async def test_a_record_without_an_mbid_writes_no_native_id_rather_than_building_an_empty_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    resolve, attach = _identity(monkeypatch)
    writer = _writer()
    processor = MusicBrainzRecordProcessor(writer, _Observer(), MagicMock())

    await processor.process_artist(MagicMock(), {"name": "Nameless"})

    # AliasRef rejects an empty external id, so a record the producer emitted without one must
    # not reach the identity calls at all.
    resolve.assert_not_awaited()
    attach.assert_not_awaited()
    assert writer.upsert_artist.await_args.args[1][16] is None


BARCODE_REF = AliasRef("barcode", "release", "077774644421")
FIRST_CATALOG_NUMBER_REF = AliasRef("catalog_number", "release", "PCS 7088")
SECOND_CATALOG_NUMBER_REF = AliasRef("catalog_number", "release", "1E 062-04243")


def _release_with_identifiers() -> dict[str, Any]:
    """A release carrying the barcode and catalogue numbers the promoted producer now sends.

    The values are written the way a dump prints them -- the barcode with grouping spaces, one
    catalogue number lower-cased and double-spaced -- so the assertions below measure the
    shared normalization rather than a pass-through.
    """
    return {
        "mbid": "release-mbid",
        "media": {},
        "barcode": "0 77774 64442 1",
        "catalog_numbers": [
            {"catalog_number": "pcs  7088", "label_mbid": "label-mbid", "label_name": "Parlophone"},
            {"catalog_number": "1E 062-04243", "label_mbid": None, "label_name": "Odeon"},
        ],
    }


@pytest.mark.asyncio
async def test_a_release_attaches_its_barcode_and_every_catalogue_number_to_its_native_id(monkeypatch: pytest.MonkeyPatch) -> None:
    musicbrainz_ref = AliasRef("musicbrainz", "release", "release-mbid")
    native_id = uuid4()
    _resolve, attach = _identity(monkeypatch, resolved={musicbrainz_ref: native_id})
    writer = _writer()
    processor = MusicBrainzRecordProcessor(writer, _Observer(), MagicMock())
    conn = MagicMock()

    await processor.process_release(conn, _release_with_identifiers())

    # One attach for the whole release, after the row it describes, on the message's own
    # connection. The barcode is compared as digits and the catalogue numbers upper-cased with
    # their whitespace collapsed, which is what makes a Discogs-sourced value resolve here.
    attach.assert_awaited_once_with(
        conn,
        {BARCODE_REF: native_id, FIRST_CATALOG_NUMBER_REF: native_id, SECOND_CATALOG_NUMBER_REF: native_id},
    )
    assert writer.upsert_release.await_args.args[1][8] == native_id
    assert attach.await_args.args[0] is conn


@pytest.mark.asyncio
async def test_a_release_with_neither_a_barcode_nor_a_catalogue_number_attaches_no_identifier_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    musicbrainz_ref = AliasRef("musicbrainz", "release", "release-mbid")
    _resolve, attach = _identity(monkeypatch, resolved={musicbrainz_ref: uuid4()})
    writer = _writer()
    processor = MusicBrainzRecordProcessor(writer, _Observer(), MagicMock())

    await processor.process_release(MagicMock(), {"mbid": "release-mbid", "media": {}, "barcode": None, "catalog_numbers": []})

    # The fields are present and empty, so there is nothing to normalize and no statement to
    # run; the release is still written with the native id it resolved.
    attach.assert_not_awaited()
    writer.upsert_release.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_identifier_alias_another_catalog_claims_is_counted_rather_than_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    musicbrainz_ref = AliasRef("musicbrainz", "release", "release-mbid")
    native_id, incumbent_id = uuid4(), uuid4()
    _resolve, attach = _identity(
        monkeypatch,
        resolved={musicbrainz_ref: native_id},
        attached={BARCODE_REF: incumbent_id, FIRST_CATALOG_NUMBER_REF: native_id, SECOND_CATALOG_NUMBER_REF: native_id},
    )
    writer = _writer()
    processor = MusicBrainzRecordProcessor(writer, _Observer(), MagicMock())

    with structlog.testing.capture_logs() as entries:
        await processor.process_release(MagicMock(), _release_with_identifiers())

    # attach_aliases never overwrites, so the barcode comes back naming the item another catalog
    # already claimed. That disagreement is counted and logged, not raised, and the release keeps
    # the id its own alias resolved.
    attach.assert_awaited_once()
    assert writer.upsert_release.await_args.args[1][8] == native_id
    conflict_logs = [entry for entry in entries if entry["conflicts"]]
    assert [(entry["log_level"], entry["attached"], entry["conflicts"]) for entry in conflict_logs] == [("warning", 3, 1)]


@pytest.mark.asyncio
async def test_a_legacy_release_event_without_the_identifier_fields_attaches_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    musicbrainz_ref = AliasRef("musicbrainz", "release", "release-mbid")
    _resolve, attach = _identity(monkeypatch, resolved={musicbrainz_ref: uuid4()})
    writer = _writer()
    processor = MusicBrainzRecordProcessor(writer, _Observer(), MagicMock())

    await processor.process_release(MagicMock(), {"mbid": "release-mbid", "media_raw": [], "status": "Official"})

    # An in-flight or dead-lettered event published before the producer carried either field
    # has no identifiers at all, and must not be rejected for it.
    attach.assert_not_awaited()
    writer.upsert_release.assert_awaited_once()
