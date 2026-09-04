"""Frozen runtime-identifier regression test for ``brainztableinator.queue_names``.

ADR 0005 (source-owned catalog ingestion repositories) freezes exchanges as
``{exchange_prefix}-{entity}`` and consumer queues as
``{exchange_prefix}-{consumer}-{entity}`` with ``.dlx``/``.dlq`` suffixes across the
producer split. This test snapshots the exact strings the previously hand-adapted
``catalog_contract`` binding produced for this service (``brainztableinator``,
source ``musicbrainz``) and asserts the local ``queue_names`` adapter, built on top
of the promoted single-source binding, still yields those same strings and that
they match the promoted contract's own ``runtime_identifiers``.
"""

import json
from pathlib import Path

from brainztableinator.brainztableinator import AMQP_CONSUMER_ID
from brainztableinator.queue_names import MUSICBRAINZ_DATA_TYPES as ENTITIES
from brainztableinator.queue_names import (
    dead_letter_exchange_name,
    dead_letter_queue_name,
    exchange_name,
    queue_name,
)


ROOT = Path(__file__).parent.parent
CONTRACT = json.loads((ROOT / "contracts" / "catalog-events" / "v1" / "contract.json").read_text())

# Snapshot of every exchange/queue/dlx/dlq name the old hand-adapted binding produced
# for this service, before the byte-for-byte promotion from musicbrainz-ingestion.
FROZEN_EXCHANGES = {
    "artists": "groovemap-musicbrainz-artists",
    "labels": "groovemap-musicbrainz-labels",
    "release-groups": "groovemap-musicbrainz-release-groups",
    "releases": "groovemap-musicbrainz-releases",
}
FROZEN_QUEUES = {
    "artists": "groovemap-musicbrainz-brainztableinator-artists",
    "labels": "groovemap-musicbrainz-brainztableinator-labels",
    "release-groups": "groovemap-musicbrainz-brainztableinator-release-groups",
    "releases": "groovemap-musicbrainz-brainztableinator-releases",
}
FROZEN_DLX = {entity: f"{queue}.dlx" for entity, queue in FROZEN_QUEUES.items()}
FROZEN_DLQ = {entity: f"{queue}.dlq" for entity, queue in FROZEN_QUEUES.items()}


def test_entity_vocabulary_is_unchanged() -> None:
    assert ENTITIES == ["artists", "labels", "release-groups", "releases"]


def test_adapter_matches_the_frozen_pre_promotion_names() -> None:
    for entity in ENTITIES:
        assert exchange_name(entity) == FROZEN_EXCHANGES[entity]
        assert queue_name(AMQP_CONSUMER_ID, entity) == FROZEN_QUEUES[entity]
        assert dead_letter_exchange_name(AMQP_CONSUMER_ID, entity) == FROZEN_DLX[entity]
        assert dead_letter_queue_name(AMQP_CONSUMER_ID, entity) == FROZEN_DLQ[entity]


def test_frozen_names_match_the_promoted_contracts_runtime_identifiers() -> None:
    runtime = CONTRACT["runtime_identifiers"]
    for entity in ENTITIES:
        assert runtime["exchanges"][entity] == FROZEN_EXCHANGES[entity]
        queue = runtime["queues"][AMQP_CONSUMER_ID][entity]
        assert queue["name"] == FROZEN_QUEUES[entity]
        assert queue["dead_letter_exchange"] == FROZEN_DLX[entity]
        assert queue["dead_letter_queue"] == FROZEN_DLQ[entity]
