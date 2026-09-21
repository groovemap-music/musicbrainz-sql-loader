"""Verify promoted contracts, generated binding, and immutable dependency pins."""

import json
import tomllib
from hashlib import sha256
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PERSISTENCE_PRODUCER_COMMIT = "06629c6a7681127f74995bbae638ba048e2abd6d"
SCHEMA_TESTED_RUNTIME_COMMIT = "6e84fe9acfd9551bd3bba2f2e78fef0ec1ef38ef"
APPLICATION_RUNTIME_COMMIT = "7abcb3ba9f467d9bdcd5b3df0b1a342a2efda73b"


def digest(path: Path) -> str:
    """Return the hexadecimal SHA-256 digest for a file."""
    return sha256(path.read_bytes()).hexdigest()


catalog_source = json.loads((ROOT / "contracts/catalog-events/v1/source.json").read_text())
persistence_source = json.loads((ROOT / "contracts/persistence/v1/source.json").read_text())
compatibility = json.loads((ROOT / "contracts/persistence/v1/compatibility.json").read_text())
runtime_compatibility = json.loads((ROOT / "contracts/runtime/compatibility.json").read_text())
lockfile = (ROOT / "uv.lock").read_text()
with (ROOT / "pyproject.toml").open("rb") as source:
    pyproject = tomllib.load(source)

assert digest(ROOT / "contracts/catalog-events/v1/contract.json") == catalog_source["contract_sha256"]
assert digest(ROOT / "brainztableinator/catalog_contract.py") == catalog_source["binding_sha256"]
assert digest(ROOT / "contracts/persistence/v1/compatibility.json") == persistence_source["contract_sha256"]
assert persistence_source["producer_commit"] == PERSISTENCE_PRODUCER_COMMIT
assert compatibility["contract"] == "groovemap.persistence"
assert compatibility["version"] == 1
assert compatibility["application_runtime"]["tested_version"] == "0.1.0"
# This is the schema owner's immutable historical test pin, not the loader's active pin.
assert compatibility["application_runtime"]["tested_commit"] == SCHEMA_TESTED_RUNTIME_COMMIT
runtime_source = pyproject["tool"]["uv"]["sources"]["groovemap-runtime"]
assert runtime_source["rev"] == APPLICATION_RUNTIME_COMMIT
assert runtime_compatibility["runtime_commit"] == APPLICATION_RUNTIME_COMMIT
assert runtime_compatibility["producer_contract_commit"] == catalog_source["producer_commit"]
assert APPLICATION_RUNTIME_COMMIT in pyproject["tool"]["uv"]["override-dependencies"][0]
assert f"python-libraries.git?rev={APPLICATION_RUNTIME_COMMIT}#{APPLICATION_RUNTIME_COMMIT}" in lockfile
