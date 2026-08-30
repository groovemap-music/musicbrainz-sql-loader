"""Identity, import, and promoted catalog-contract smoke tests."""

from pathlib import Path

import brainztableinator.brainztableinator as service
from brainztableinator.catalog_contract import AMQP_EXCHANGE_TYPE, MUSICBRAINZ_DATA_TYPES, MUSICBRAINZ_EXCHANGE_PREFIX
from brainztableinator.config import BrainztableinatorConfig, MusicBrainzSQLLoaderConfig


ROOT = Path(__file__).parent.parent


def test_service_import_exposes_entry_point() -> None:
    assert callable(service.main)


def test_public_runtime_identity_uses_repository_name() -> None:
    assert service.SERVICE_NAME == "musicbrainz-sql-loader"
    assert "musicbrainz-sql-loader" in service.STARTUP_BANNER
    assert service.get_health_data()["service"] == "musicbrainz-sql-loader"


def test_legacy_python_and_amqp_identifiers_remain_compatible() -> None:
    assert BrainztableinatorConfig is MusicBrainzSQLLoaderConfig
    assert service.AMQP_CONSUMER_ID == "brainztableinator"


def test_catalog_contract_matches_musicbrainz_stream() -> None:
    assert MUSICBRAINZ_EXCHANGE_PREFIX == "groovemap-musicbrainz"
    assert AMQP_EXCHANGE_TYPE == "fanout"
    assert MUSICBRAINZ_DATA_TYPES == ["artists", "labels", "release-groups", "releases"]


def test_operator_docs_use_current_identity_and_mermaid_diagrams() -> None:
    operator_docs = [ROOT / "README.md", *(ROOT / "docs").glob("*.md"), ROOT / "brainztableinator" / "README.md"]
    combined = "\n".join(path.read_text() for path in operator_docs if path.name != "extraction.md")
    assert "discogsography" not in combined.casefold()
    assert "```mermaid" in (ROOT / "README.md").read_text()
