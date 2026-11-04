#!/usr/bin/env bash
# Start a disposable PostgreSQL on a random loopback port and run the write benchmark against it.
# Extra arguments go to scripts/bench-write.py (--records, --workers, --latency-ms, --seed).
set -euo pipefail

postgres_image="postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2"
container=""

cleanup() {
    if [[ -n "${container}" ]]; then
        docker stop "${container}" >/dev/null
    fi
}
trap cleanup EXIT

if [[ -z "${BENCH_DATABASE_URL:-}" ]]; then
    if ! command -v docker >/dev/null 2>&1; then
        echo "Docker is required when BENCH_DATABASE_URL is unset" >&2
        exit 1
    fi

    container="musicbrainz-sql-loader-bench-$$"
    # --rm removes the container and its anonymous volume when it stops.
    docker run \
        --detach \
        --env POSTGRES_PASSWORD=bench \
        --name "${container}" \
        --publish 127.0.0.1::5432 \
        --rm \
        "${postgres_image}" >/dev/null

    ready="false"
    for _attempt in $(seq 1 30); do
        if docker exec "${container}" pg_isready --quiet --username postgres; then
            ready="true"
            break
        fi
        sleep 1
    done
    if [[ "${ready}" != "true" ]]; then
        echo "Disposable PostgreSQL did not become ready" >&2
        exit 1
    fi
    # The image restarts once while initializing; wait for it to settle.
    sleep 2

    published_address="$(docker port "${container}" 5432/tcp | head -n 1)"
    BENCH_DATABASE_URL="postgresql://postgres:bench@127.0.0.1:${published_address##*:}/postgres"
    export BENCH_DATABASE_URL
fi

uv run python scripts/bench-write.py "$@"
