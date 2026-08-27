set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

setup:
    uv sync --dev --frozen

source-check:
    uvx --from ruff==0.16.4 ruff format --check .
    uvx --from ruff==0.16.4 ruff check .
    python scripts/check-contracts.py
    gitleaks git --redact --no-banner
    gitleaks dir . --redact --no-banner

check: source-check typecheck test build install-check license-check bump-preview

format:
    uv run ruff format .
    uv run ruff check --fix .

typecheck:
    uv run mypy

test:
    uv run pytest --cov=brainztableinator --cov-report=term-missing

build:
    uv build --out-dir dist --clear

install-check: build
    bash scripts/install-check.sh

license-check:
    uv run python scripts/check-license.py
    uv run pip-licenses --fail-on "GPL-2.0-only;GPL-3.0-only;AGPL-3.0-only"

audit:
    uv run pip-audit

prepare-runtime-wheel:
    bash scripts/prepare-runtime-wheel.sh

image: prepare-runtime-wheel
    bash scripts/build-image.sh
    docker run --rm --entrypoint /app/.venv/bin/python musicbrainz-sql-loader:local -c 'import brainztableinator.brainztableinator'
    test "$(docker run --rm --entrypoint /usr/bin/id musicbrainz-sql-loader:local -u):$(docker run --rm --entrypoint /usr/bin/id musicbrainz-sql-loader:local -g)" = "1000:1000"

bump-preview:
    uv run cz bump --dry-run --changelog --yes --check-consistency

# Update local version metadata and changelog only; do not commit, tag, push, or publish.
bump:
    uv run cz bump --version-files-only --changelog --yes --check-consistency
    uv lock

release-dry-run: check
    bash scripts/release-dry-run.sh
