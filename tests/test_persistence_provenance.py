"""Regression coverage for the promoted persistence contract's provenance."""

import json
import shutil
import subprocess
from hashlib import sha256
from pathlib import Path

import pytest


ROOT = Path(__file__).parent.parent
SOURCE_PATH = ROOT / "contracts/persistence/v1/source.json"
CONTRACT_PATH = ROOT / "contracts/persistence/v1/compatibility.json"
PRODUCER_CONTRACT_PATH = "contracts/persistence/v1/compatibility.json"
PRODUCER_COMMIT = "91cbf6e3712a2fa403693b5c81fde729f29cb4ce"
KNOWN_MISMATCHING_COMMIT = "c1c0ecc479f6ae612792bc72d2cf34d4b443dc4e"
GIT = shutil.which("git")
assert GIT is not None


def _source() -> dict[str, str]:
    return json.loads(SOURCE_PATH.read_text())


def _verify_source(source: dict[str, str]) -> None:
    assert source["producer_repository"] == "https://github.com/groovemap-music/database-schema"
    assert source["producer_commit"] == PRODUCER_COMMIT
    assert sha256(CONTRACT_PATH.read_bytes()).hexdigest() == source["contract_sha256"]


def _local_database_schema_repository() -> Path | None:
    common_dir = subprocess.run(  # noqa: S603 - arguments are fixed and Git is resolved above
        [GIT, "rev-parse", "--path-format=absolute", "--git-common-dir"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    candidate = Path(common_dir.stdout.strip()).parent.parent / "database-schema"
    return candidate if candidate.is_dir() else None


def test_persistence_source_rejects_known_mismatching_revision() -> None:
    source = _source()
    _verify_source(source)

    mismatching_source = source | {"producer_commit": KNOWN_MISMATCHING_COMMIT}
    with pytest.raises(AssertionError):
        _verify_source(mismatching_source)


def test_persistence_source_matches_local_producer_object_when_available() -> None:
    repository = _local_database_schema_repository()
    if repository is None:
        pytest.skip("database-schema is not available in this local Git workspace")

    source = _source()
    _verify_source(source)
    object_name = f"{source['producer_commit']}:{PRODUCER_CONTRACT_PATH}"
    producer = subprocess.run(  # noqa: S603 - the exact revision and repository-relative path are fixed above
        [GIT, "cat-file", "blob", object_name],
        cwd=repository,
        check=False,
        capture_output=True,
    )
    if producer.returncode != 0:
        pytest.skip("the recorded database-schema producer object is not available locally")

    assert producer.stdout == CONTRACT_PATH.read_bytes()
