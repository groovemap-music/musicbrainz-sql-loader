"""The reload seen lists: how a reload proves a child row is still upstream without rewriting it.

The stale-row purge deletes the ``musicbrainz.relationships`` and ``musicbrainz.external_links``
rows an extraction did not reach. It used to learn that from ``updated_at`` alone, so every upsert
rewrote its row on every conflict, changed or not, and a reload cost as much as the first load.
``groovemap-database-schema`` adds a seen list per child table, in the ``loader_seen`` schema: the
loader appends each row's key in the message's transaction, and the purge deletes a row only when
it is older than the extraction **and** not in the list. The design, its measurements and its
safety argument are in database-schema's ``docs/architecture.md``, "Reload seen lists", and the
persistence contract's ``reload_seen_lists`` entry. This module holds the loader's half of it: the
mode, the startup probe, and the append statements.

Three modes, chosen by ``RELOAD_MODE`` and narrowed by the probe:

* ``touch``: today's behaviour. Every child-row conflict refreshes ``updated_at`` and the purge
  deletes by it. Nothing is appended. This is also what runs whenever the schema has no
  ``loader_seen`` objects, so the loader can ship before the schema does.
* ``shadow``: the touch and the ``updated_at`` purge exactly as in ``touch``, plus the appends. At
  each purge it logs ``loader_seen.<list>_shadow`` for both lists, so one full reload shows whether
  the seen purge would delete exactly what the touch purge does. Then it restarts both lists.
* ``seen``: the child upserts stop rewriting unchanged rows, and the purge deletes by
  ``loader_seen.<list>_stale``. Switching to it needs operator approval after a matching shadow
  reload (``refreshed_not_seen = 0`` and ``touch_purge = seen_purge`` on both lists).

The entity upserts skip unchanged rows in every mode: the MusicBrainz entity tables are not
purged, so nothing reads their ``updated_at`` as evidence of a sighting.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Final


SEEN_LIST_LOADER: Final = "musicbrainz"


class ReloadMode(StrEnum):
    """How a reload proves a child row is still upstream: see the module docstring."""

    TOUCH = "touch"
    SHADOW = "shadow"
    SEEN = "seen"

    @property
    def appends(self) -> bool:
        """Whether child-row writes append their keys to the seen lists."""
        return self is not ReloadMode.TOUCH

    @property
    def touches(self) -> bool:
        """Whether every child-row conflict refreshes ``updated_at``, changed or not."""
        return self is not ReloadMode.SEEN


# The default once the schema carries the lists. `seen` is never a default: it needs a matching
# shadow reload and the operator's approval first.
DEFAULT_RELOAD_MODE: Final = ReloadMode.SHADOW


def parse_reload_mode(raw: str | None) -> ReloadMode:
    """Return the configured mode, the default when unset; raise ``ValueError`` on anything else."""
    if raw is None or not raw.strip():
        return DEFAULT_RELOAD_MODE
    try:
        return ReloadMode(raw.strip().lower())
    except ValueError:
        choices = ", ".join(mode.value for mode in ReloadMode)
        raise ValueError(f"RELOAD_MODE must be one of {choices}, got {raw!r}") from None


# The seen list of each reconciled table, keyed by the table's name. The data type is the name
# `loader_seen.start`, `intact` and `status` take, and `loader_seen.musicbrainz_<data type>` is the
# list itself.
SEEN_LIST_DATA_TYPES: Final[dict[str, str]] = {
    "relationships": "relationships",
    "external_links": "external_links",
}


def seen_list_relation(data_type: str) -> str:
    """Return the list relation for one data type, e.g. ``loader_seen.musicbrainz_relationships``."""
    return f"loader_seen.{SEEN_LIST_LOADER}_{data_type}"


# Every object the loader calls, by the signature `to_regprocedure` resolves. The probe requires
# all of them, so a schema that is part-way through the upgrade runs in touch mode.
_REQUIRED_RELATIONS: Final = ("loader_seen.lists", *(seen_list_relation(data_type) for data_type in SEEN_LIST_DATA_TYPES.values()))
_REQUIRED_FUNCTIONS: Final = (
    "loader_seen.start(text, text)",
    "loader_seen.intact(text, text, timestamp with time zone)",
    "loader_seen.status(text, text, timestamp with time zone)",
    "loader_seen.hash_key(bytea)",
    "musicbrainz.external_link_key(uuid, text, text, text)",
    *(
        f"loader_seen.{SEEN_LIST_LOADER}_{data_type}_{suffix}"
        for data_type in SEEN_LIST_DATA_TYPES.values()
        for suffix in ("stale(timestamp with time zone)", "shadow(timestamp with time zone, integer)")
    ),
)

_MISSING_OBJECTS = """
SELECT name FROM unnest(%(relations)s::text[]) AS name WHERE pg_catalog.to_regclass(name) IS NULL
UNION ALL
SELECT name FROM unnest(%(functions)s::text[]) AS name WHERE pg_catalog.to_regprocedure(name) IS NULL
"""


async def missing_seen_list_objects(connection_pool: Any) -> list[str]:
    """Return the ``loader_seen`` objects the schema does not declare, empty when all are there."""
    async with connection_pool.connection() as conn, conn.cursor() as cursor:
        await cursor.execute(_MISSING_OBJECTS, {"relations": list(_REQUIRED_RELATIONS), "functions": list(_REQUIRED_FUNCTIONS)})
        rows = await cursor.fetchall()
    return [str(row[0]) for row in rows]


async def resolve_reload_mode(connection_pool: Any, logger: Any, configured: ReloadMode) -> ReloadMode:
    """Return the mode this run uses: the configured one, or ``touch`` when the schema lacks the lists.

    ``touch`` is today's behaviour, so falling back to it is always safe: the loader keeps
    touching and purging by ``updated_at``. A probe that fails falls back the same way.
    """
    if configured is ReloadMode.TOUCH:
        logger.info("🧭 Reload mode touch: child rows are refreshed on every conflict and purged by updated_at", reload_mode=configured.value)
        return ReloadMode.TOUCH
    try:
        missing = await missing_seen_list_objects(connection_pool)
    except Exception as error:
        logger.error(
            f"❌ Could not probe for the reload seen lists, running in touch mode instead of {configured.value}",
            configured=configured.value,
            error_type=type(error).__name__,
        )
        return ReloadMode.TOUCH
    if missing:
        logger.warning(
            f"⚠️ Reload seen lists absent, running in touch mode instead of {configured.value}; "
            f"the mode applies once the schema carrying loader_seen is deployed",
            configured=configured.value,
            missing=missing,
        )
        return ReloadMode.TOUCH
    logger.info(f"🧭 Reload mode {configured.value}", reload_mode=configured.value)
    return configured


# The appends, one statement per list per message, in the message's own transaction. Each key is
# computed in the database by the functions the list's purge uses, over the values the upsert
# wrote, so the list and the stale set can never disagree about how a row is keyed. Duplicates
# (a relationship seen from both endpoints, a redelivered message) are expected and harmless.
#
# The relationship key's arguments are in `musicbrainz.relationship_key`'s order, which is not the
# insert's column order.
RELATIONSHIP_SEEN_APPEND: Final = (
    f"INSERT INTO {seen_list_relation('relationships')} (seen_key) "  # noqa: S608 -- module constants only
    "SELECT loader_seen.hash_key(musicbrainz.relationship_key("
    "k.source_mbid, k.target_mbid, k.source_entity_type, k.target_entity_type, k.relationship_type, k.begin_date, k.end_date, k.attributes)) "
    "FROM unnest(%s::uuid[], %s::uuid[], %s::text[], %s::text[], %s::text[], %s::text[], %s::text[], %s::jsonb[]) "
    "AS k(source_mbid, target_mbid, source_entity_type, target_entity_type, relationship_type, begin_date, end_date, attributes)"
)

EXTERNAL_LINK_SEEN_APPEND: Final = (
    f"INSERT INTO {seen_list_relation('external_links')} (seen_key) "  # noqa: S608 -- module constants only
    "SELECT loader_seen.hash_key(musicbrainz.external_link_key(k.mbid, k.entity_type, k.service_name, k.url)) "
    "FROM unnest(%s::uuid[], %s::text[], %s::text[], %s::text[]) AS k(mbid, entity_type, service_name, url)"
)
