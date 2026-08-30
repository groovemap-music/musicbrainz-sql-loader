"""Import and promoted catalog-contract smoke tests."""

from inspect import signature
from typing import get_type_hints

from aio_pika.abc import AbstractIncomingMessage

import brainztableinator.brainztableinator as service
from brainztableinator.catalog_contract import AMQP_EXCHANGE_TYPE, MUSICBRAINZ_DATA_TYPES, MUSICBRAINZ_EXCHANGE_PREFIX


def test_service_import_exposes_entry_point() -> None:
    assert callable(service.main)


def test_message_annotation_supports_runtime_introspection() -> None:
    """Keep the message type available to Python's runtime annotation APIs."""
    assert signature(service.on_data_message).parameters["message"].annotation is AbstractIncomingMessage
    assert get_type_hints(service.on_data_message)["message"] is AbstractIncomingMessage


def test_catalog_contract_matches_musicbrainz_stream() -> None:
    assert MUSICBRAINZ_EXCHANGE_PREFIX == "groovemap-musicbrainz"
    assert AMQP_EXCHANGE_TYPE == "fanout"
    assert MUSICBRAINZ_DATA_TYPES == ["artists", "labels", "release-groups", "releases"]
