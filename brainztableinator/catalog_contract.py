"""Generated from contracts/catalog-events/definitions/musicbrainz.json; do not edit."""

from os import getenv

CONTRACT_NAME = "groovemap.catalog-events"
CONTRACT_VERSION = 1
SOURCE = "musicbrainz"
AMQP_EXCHANGE_TYPE = "fanout"
ENTITY_TYPES = ["artists", "labels", "release-groups", "releases"]
CONSUMERS = ["brainzgraphinator", "brainztableinator"]
EXCHANGE_PREFIX = getenv("MUSICBRAINZ_EXCHANGE_PREFIX", "groovemap-musicbrainz")


def exchange_name(entity: str) -> str:
    if entity not in ENTITY_TYPES:
        raise ValueError(f"Unknown MusicBrainz entity: {entity}")
    return f"{EXCHANGE_PREFIX}-{entity}"


def queue_name(consumer: str, entity: str) -> str:
    if consumer not in CONSUMERS:
        raise ValueError(f"Unknown MusicBrainz consumer: {consumer}")
    if entity not in ENTITY_TYPES:
        raise ValueError(f"Unknown MusicBrainz entity: {entity}")
    return f"{EXCHANGE_PREFIX}-{consumer}-{entity}"
