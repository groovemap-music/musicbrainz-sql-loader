"""Verify promoted contracts, generated binding, and immutable dependency pins."""

import json
import tomllib
from hashlib import sha256
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PERSISTENCE_PRODUCER_COMMIT = "ea36cfa66672cb1e3f565165fea56d01b9b19c95"
APPLICATION_RUNTIME_COMMIT = "6e84fe9acfd9551bd3bba2f2e78fef0ec1ef38ef"


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
# The schema owner records the runtime revision it tested the persistence contract against;
# pinning a different one would run the loader on an untested pairing.
assert compatibility["application_runtime"]["tested_commit"] == APPLICATION_RUNTIME_COMMIT
runtime_source = pyproject["tool"]["uv"]["sources"]["groovemap-runtime"]
assert runtime_source["rev"] == APPLICATION_RUNTIME_COMMIT
