"""Private MusicBrainz mapping and batch coordination."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Protocol


if TYPE_CHECKING:
    from collections.abc import Callable

    from brainztableinator._persistence import MusicBrainzWriter


class BatchObserver(Protocol):
    """Telemetry operations surrounding a relationship or link batch."""

    def flush(self, entity: str) -> Any: ...

    def record(self, entity: str, size: int, duration_s: float, outcome: str) -> None: ...

    def set_outcome(self, span: Any, outcome: str) -> None: ...


_ENTITY_TYPE_ALIASES = {"release_group": "release-group"}


def _get_or(record: dict[str, Any], key: str, default: Any) -> Any:
    """Return the default when a producer emitted an explicit JSON null."""
    value = record.get(key)
    return default if value is None else value


def _life_span(record: dict[str, Any]) -> dict[str, Any]:
    life_span = record.get("life_span")
    return life_span if isinstance(life_span, dict) else {}


def _canonical_entity_type(entity_type: str) -> str:
    """Normalize the legacy producer spelling used by in-flight or DLQ events."""
    return _ENTITY_TYPE_ALIASES.get(entity_type, entity_type)


class MusicBrainzRecordProcessor:
    """Map catalog records and coordinate their child-row batches."""

    def __init__(
        self,
        writer: MusicBrainzWriter,
        observer: BatchObserver,
        media_mapper: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        self._writer = writer
        self._observer = observer
        self._media_mapper = media_mapper

    async def insert_relationships(
        self,
        conn: Any,
        source_mbid: str,
        source_type: str,
        relationships: list[dict[str, Any]],
    ) -> None:
        canonical_source_type = _canonical_entity_type(source_type)
        rows = []
        for relationship in relationships:
            if not (relationship.get("target_mbid") and relationship.get("target_type") and relationship.get("type")):
                continue

            target_type = _canonical_entity_type(relationship["target_type"])
            if relationship.get("direction") == "backward":
                row_source_mbid, row_source_type = relationship["target_mbid"], target_type
                row_target_mbid, row_target_type = source_mbid, canonical_source_type
            else:
                row_source_mbid, row_source_type = source_mbid, canonical_source_type
                row_target_mbid, row_target_type = relationship["target_mbid"], target_type

            rows.append(
                (
                    row_source_mbid,
                    row_source_type,
                    row_target_mbid,
                    row_target_type,
                    relationship.get("type", ""),
                    _get_or(relationship, "attributes", []),
                    relationship.get("begin_date"),
                    relationship.get("end_date"),
                    _get_or(relationship, "ended", False),
                )
            )

        with self._observer.flush(canonical_source_type) as span:
            if not rows:
                self._observer.record(canonical_source_type, 0, 0.0, "skipped")
                self._observer.set_outcome(span, "skipped")
                return
            started = time.perf_counter()
            try:
                await self._writer.insert_relationships(conn, rows)
            except Exception:
                self._observer.record(canonical_source_type, len(rows), time.perf_counter() - started, "failed")
                self._observer.set_outcome(span, "failed")
                raise
            self._observer.record(canonical_source_type, len(rows), time.perf_counter() - started, "processed")
            self._observer.set_outcome(span, "processed")

    async def insert_external_links(
        self,
        conn: Any,
        mbid: str,
        entity_type: str,
        links: list[dict[str, Any]],
    ) -> None:
        canonical_entity_type = _canonical_entity_type(entity_type)
        rows = [(mbid, entity_type, link.get("url", ""), link.get("service", "")) for link in links if link.get("url") and link.get("service")]
        with self._observer.flush(canonical_entity_type) as span:
            if not rows:
                self._observer.record(canonical_entity_type, 0, 0.0, "skipped")
                self._observer.set_outcome(span, "skipped")
                return
            started = time.perf_counter()
            try:
                await self._writer.insert_external_links(conn, rows)
            except Exception:
                self._observer.record(canonical_entity_type, len(rows), time.perf_counter() - started, "failed")
                self._observer.set_outcome(span, "failed")
                raise
            self._observer.record(canonical_entity_type, len(rows), time.perf_counter() - started, "processed")
            self._observer.set_outcome(span, "processed")

    async def process_artist(self, conn: Any, record: dict[str, Any]) -> None:
        mbid = record.get("mbid", record.get("id", ""))
        life_span = _life_span(record)
        await self._writer.upsert_artist(
            conn,
            (
                mbid,
                record.get("name", ""),
                record.get("sort_name", ""),
                record.get("mb_type", ""),
                record.get("gender", ""),
                _get_or(record, "begin_date", life_span.get("begin")),
                _get_or(record, "end_date", life_span.get("end")),
                _get_or(record, "ended", _get_or(life_span, "ended", False)),
                record.get("area", ""),
                record.get("begin_area", ""),
                record.get("end_area", ""),
                record.get("disambiguation", ""),
                record.get("discogs_artist_id"),
                _get_or(record, "aliases", []),
                _get_or(record, "tags", []),
                record,
            ),
        )
        await self.insert_relationships(conn, mbid, "artist", record.get("relations", []))
        await self.insert_external_links(conn, mbid, "artist", record.get("external_links", []))

    async def process_label(self, conn: Any, record: dict[str, Any]) -> None:
        mbid = record.get("mbid", record.get("id", ""))
        life_span = _life_span(record)
        await self._writer.upsert_label(
            conn,
            (
                mbid,
                record.get("name", ""),
                record.get("mb_type", ""),
                record.get("label_code"),
                _get_or(record, "begin_date", life_span.get("begin")),
                _get_or(record, "end_date", life_span.get("end")),
                _get_or(record, "ended", _get_or(life_span, "ended", False)),
                record.get("area", ""),
                record.get("disambiguation", ""),
                record.get("discogs_label_id"),
                record,
            ),
        )
        await self.insert_relationships(conn, mbid, "label", record.get("relations", []))
        await self.insert_external_links(conn, mbid, "label", record.get("external_links", []))

    def release_media_block(self, record: dict[str, Any]) -> dict[str, Any]:
        """Use canonical producer media, deriving it only for legacy events."""
        media = record.get("media")
        if isinstance(media, dict):
            return media
        return self._media_mapper(
            {
                "media": record.get("media_raw") or [],
                "status": record.get("status"),
                "packaging": record.get("packaging"),
                "release_group": record.get("release_group"),
            }
        )

    async def process_release(self, conn: Any, record: dict[str, Any]) -> None:
        mbid = record.get("mbid", record.get("id", ""))
        await self._writer.upsert_release(
            conn,
            (
                mbid,
                record.get("name", ""),
                record.get("barcode"),
                record.get("status", ""),
                record.get("release_group_mbid"),
                record.get("discogs_release_id"),
                self.release_media_block(record),
                record,
            ),
        )
        await self.insert_relationships(conn, mbid, "release", record.get("relations", []))
        await self.insert_external_links(conn, mbid, "release", record.get("external_links", []))

    async def process_release_group(self, conn: Any, record: dict[str, Any]) -> None:
        mbid = record.get("mbid", record.get("id", ""))
        await self._writer.upsert_release_group(
            conn,
            (
                mbid,
                record.get("name", ""),
                record.get("mb_type", ""),
                _get_or(record, "secondary_types", []),
                record.get("first_release_date"),
                record.get("disambiguation", ""),
                record.get("discogs_master_id"),
                record,
            ),
        )
        await self.insert_relationships(conn, mbid, "release-group", record.get("relations", []))
        await self.insert_external_links(conn, mbid, "release-group", record.get("external_links", []))
