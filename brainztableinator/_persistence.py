"""Private PostgreSQL writes for normalized MusicBrainz records."""

from typing import Any, Protocol

from psycopg.types.json import Jsonb


class MusicBrainzWriter(Protocol):
    """Database operations required by MusicBrainz record processing."""

    async def upsert_artist(self, conn: Any, values: tuple[Any, ...]) -> None: ...

    async def upsert_label(self, conn: Any, values: tuple[Any, ...]) -> None: ...

    async def upsert_release(self, conn: Any, values: tuple[Any, ...]) -> None: ...

    async def upsert_release_group(self, conn: Any, values: tuple[Any, ...]) -> None: ...

    async def insert_relationships(self, conn: Any, rows: list[tuple[Any, ...]]) -> None: ...

    async def insert_external_links(self, conn: Any, rows: list[tuple[Any, ...]]) -> None: ...


class PostgreSQLMusicBrainzWriter:
    """Execute the MusicBrainz schema's entity-specific write statements."""

    async def upsert_artist(self, conn: Any, values: tuple[Any, ...]) -> None:
        params = (*values[:13], Jsonb(values[13]), Jsonb(values[14]), Jsonb(values[15]))
        async with conn.cursor() as cursor:
            await cursor.execute(
                "INSERT INTO musicbrainz.artists "
                "(mbid, name, sort_name, type, gender, begin_date, end_date, ended, "
                "area, begin_area, end_area, disambiguation, discogs_artist_id, aliases, tags, data) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (mbid) DO UPDATE SET "
                "name = EXCLUDED.name, sort_name = EXCLUDED.sort_name, "
                "type = EXCLUDED.type, gender = EXCLUDED.gender, "
                "begin_date = EXCLUDED.begin_date, end_date = EXCLUDED.end_date, "
                "ended = EXCLUDED.ended, area = EXCLUDED.area, "
                "begin_area = EXCLUDED.begin_area, end_area = EXCLUDED.end_area, "
                "disambiguation = EXCLUDED.disambiguation, "
                "discogs_artist_id = EXCLUDED.discogs_artist_id, "
                "aliases = EXCLUDED.aliases, tags = EXCLUDED.tags, "
                "data = EXCLUDED.data, updated_at = NOW()",
                params,
            )

    async def upsert_label(self, conn: Any, values: tuple[Any, ...]) -> None:
        params = (*values[:10], Jsonb(values[10]))
        async with conn.cursor() as cursor:
            await cursor.execute(
                "INSERT INTO musicbrainz.labels "
                "(mbid, name, type, label_code, begin_date, end_date, ended, area, disambiguation, discogs_label_id, data) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (mbid) DO UPDATE SET "
                "name = EXCLUDED.name, type = EXCLUDED.type, "
                "label_code = EXCLUDED.label_code, "
                "begin_date = EXCLUDED.begin_date, end_date = EXCLUDED.end_date, "
                "ended = EXCLUDED.ended, area = EXCLUDED.area, "
                "disambiguation = EXCLUDED.disambiguation, "
                "discogs_label_id = EXCLUDED.discogs_label_id, "
                "data = EXCLUDED.data, updated_at = NOW()",
                params,
            )

    async def upsert_release(self, conn: Any, values: tuple[Any, ...]) -> None:
        params = (*values[:6], Jsonb(values[6]), Jsonb(values[7]))
        async with conn.cursor() as cursor:
            await cursor.execute(
                "INSERT INTO musicbrainz.releases "
                "(mbid, name, barcode, status, release_group_mbid, discogs_release_id, media, data) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (mbid) DO UPDATE SET "
                "name = EXCLUDED.name, barcode = EXCLUDED.barcode, "
                "status = EXCLUDED.status, "
                "release_group_mbid = EXCLUDED.release_group_mbid, "
                "discogs_release_id = EXCLUDED.discogs_release_id, "
                "media = EXCLUDED.media, "
                "data = EXCLUDED.data, updated_at = NOW()",
                params,
            )

    async def upsert_release_group(self, conn: Any, values: tuple[Any, ...]) -> None:
        params = (*values[:3], Jsonb(values[3]), *values[4:7], Jsonb(values[7]))
        async with conn.cursor() as cursor:
            await cursor.execute(
                "INSERT INTO musicbrainz.release_groups "
                "(mbid, name, type, secondary_types, first_release_date, disambiguation, discogs_master_id, data) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (mbid) DO UPDATE SET "
                "name = EXCLUDED.name, type = EXCLUDED.type, "
                "secondary_types = EXCLUDED.secondary_types, "
                "first_release_date = EXCLUDED.first_release_date, "
                "disambiguation = EXCLUDED.disambiguation, "
                "discogs_master_id = EXCLUDED.discogs_master_id, "
                "data = EXCLUDED.data, updated_at = NOW()",
                params,
            )

    async def insert_relationships(self, conn: Any, rows: list[tuple[Any, ...]]) -> None:
        params = [(*row[:5], Jsonb(row[5]), *row[6:]) for row in rows]
        async with conn.cursor() as cursor:
            await cursor.executemany(
                "INSERT INTO musicbrainz.relationships "
                "(source_mbid, source_entity_type, target_mbid, target_entity_type, relationship_type, attributes, begin_date, end_date, ended) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                # This conflict target is the relationships_natural_key contract in
                # database-schema; dates and attributes distinguish separate relationships.
                "ON CONFLICT (source_mbid, target_mbid, source_entity_type, target_entity_type, relationship_type, begin_date, end_date, attributes) "
                "DO UPDATE SET ended = EXCLUDED.ended",
                params,
            )

    async def insert_external_links(self, conn: Any, rows: list[tuple[Any, ...]]) -> None:
        async with conn.cursor() as cursor:
            await cursor.executemany(
                "INSERT INTO musicbrainz.external_links "
                "(mbid, entity_type, url, service_name) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (mbid, entity_type, service_name, url) DO UPDATE SET url = EXCLUDED.url",
                rows,
            )
