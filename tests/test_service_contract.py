"""Import and promoted catalog-contract smoke tests."""

import brainztableinator.brainztableinator as service
from brainztableinator.catalog_contract import AMQP_EXCHANGE_TYPE, MUSICBRAINZ_DATA_TYPES, MUSICBRAINZ_EXCHANGE_PREFIX


def test_service_import_exposes_entry_point() -> None:
    assert callable(service.main)


def test_catalog_contract_matches_musicbrainz_stream() -> None:
    assert MUSICBRAINZ_EXCHANGE_PREFIX == "groovemap-musicbrainz"
    assert AMQP_EXCHANGE_TYPE == "fanout"
    assert MUSICBRAINZ_DATA_TYPES == ["artists", "labels", "release-groups", "releases"]
