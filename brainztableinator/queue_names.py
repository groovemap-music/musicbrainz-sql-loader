"""Local adapter over the generated ``catalog_contract`` binding.

``brainztableinator/catalog_contract.py`` is promoted byte-for-byte from the
source-owned ``musicbrainz-ingestion`` producer contract (see ADR 0005:
source-owned catalog ingestion repositories) and is not edited to fit this
consumer. Its binding is musicbrainz-only and exposes a one-argument
``exchange_name(entity)``, ``queue_name(consumer, entity)``, and no
dead-letter helpers or per-consumer source registry.

This module adapts that binding to the shapes this service's runtime code
needs, and is the single place that reconstructs the dead-letter naming this
service always used: ``f"{queue_name(...)}.dlx"`` / ``".dlq"``. ADR 0005
freezes exchanges as ``{exchange_prefix}-{entity}`` and consumer queues as
``{exchange_prefix}-{consumer}-{entity}`` with ``.dlx``/``.dlq`` suffixes, so
these names are unchanged from the previously hand-adapted binding; see
``tests/test_queue_names.py`` for the frozen-value regression test.
"""

from __future__ import annotations

from brainztableinator.catalog_contract import (
    AMQP_EXCHANGE_TYPE,
    CONSUMERS,
    ENTITY_TYPES,
    EXCHANGE_PREFIX,
    SOURCE,
    exchange_name,
    queue_name,
)


# Renamed for this service: the generated binding calls these ENTITY_TYPES and
# EXCHANGE_PREFIX; the service historically referred to them by source name.
MUSICBRAINZ_DATA_TYPES = ENTITY_TYPES
MUSICBRAINZ_EXCHANGE_PREFIX = EXCHANGE_PREFIX
PIPELINE_SOURCE = SOURCE
CONSUMER_SOURCES = {consumer: {"source": SOURCE} for consumer in CONSUMERS}


def dead_letter_exchange_name(consumer: str, entity: str) -> str:
    """Build the dead-letter exchange name for a consumer queue."""
    return f"{queue_name(consumer, entity)}.dlx"


def dead_letter_queue_name(consumer: str, entity: str) -> str:
    """Build the dead-letter queue name for a consumer queue."""
    return f"{queue_name(consumer, entity)}.dlq"


__all__ = [
    "AMQP_EXCHANGE_TYPE",
    "CONSUMER_SOURCES",
    "MUSICBRAINZ_DATA_TYPES",
    "MUSICBRAINZ_EXCHANGE_PREFIX",
    "PIPELINE_SOURCE",
    "dead_letter_exchange_name",
    "dead_letter_queue_name",
    "exchange_name",
    "queue_name",
]
