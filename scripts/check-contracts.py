"""Verify promoted contracts, generated binding, and immutable dependency pins."""

import json
import tomllib
from hashlib import sha256
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PERSISTENCE_PRODUCER_COMMIT = "b67bc222b31b39205db51f7fac4b52663aac2cf7"
APPLICATION_RUNTIME_COMMIT = "24704f5fd48d3ef4fff29398585e9924e225b0c5"


def digest(path: Path) -> str:
    """Return the hexadecimal SHA-256 digest for a file."""
    return sha256(path.read_bytes()).hexdigest()


catalog_source = json.loads((ROOT / "contracts/catalog-events/v1/source.json").read_text())
persistence_source = json.loads((ROOT / "contracts/persistence/v1/source.json").read_text())
compatibility = json.loads((ROOT / "contracts/persistence/v1/compatibility.json").read_text())
with (ROOT / "pyproject.toml").open("rb") as source:
    pyproject = tomllib.load(source)

assert digest(ROOT / "contracts/catalog-events/v1/contract.json") == catalog_source["contract_sha256"]
assert digest(ROOT / "brainztableinator/catalog_contract.py") == catalog_source["binding_sha256"]
assert digest(ROOT / "contracts/persistence/v1/compatibility.json") == persistence_source["contract_sha256"]
assert persistence_source["producer_commit"] == PERSISTENCE_PRODUCER_COMMIT
assert compatibility["contract"] == "groovemap.persistence"
assert compatibility["version"] == 1
assert compatibility["application_runtime"]["tested_version"] == "0.1.0"
runtime_source = pyproject["tool"]["uv"]["sources"]["groovemap-runtime"]
assert runtime_source["rev"] == APPLICATION_RUNTIME_COMMIT
