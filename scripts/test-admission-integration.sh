#!/usr/bin/env bash
set -euo pipefail

postgres_image="postgres:18-alpine@sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2"
rabbitmq_image="rabbitmq:4.1-management-alpine@sha256:54ef5ff505352c1beef05a97ee9bd6ab95b3559a76629ba0119b1d63d8d6e4b2"
postgres_container="musicbrainz-admission-postgres-$$"
rabbitmq_container="musicbrainz-admission-rabbitmq-$$"
config_dir="$(mktemp -d)"

cleanup() {
    docker rm --force "${postgres_container}" "${rabbitmq_container}" >/dev/null 2>&1 || true
    rm -rf "${config_dir}"
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
    echo "Docker is required for the owned admission integration lane" >&2
    exit 1
fi

# Deny global QoS in the disposable broker so any accidental global QoS call fails.
cat >"${config_dir}/rabbitmq.conf" <<'EOF'
deprecated_features.permit.global_qos = false
EOF

docker run --detach \
    --name "${postgres_container}" \
    --env POSTGRES_PASSWORD=integration-test \
    --publish 127.0.0.1::5432 \
    --rm \
    "${postgres_image}" >/dev/null

docker create \
    --name "${rabbitmq_container}" \
    --publish 127.0.0.1::5672 \
    "${rabbitmq_image}" >/dev/null
docker cp "${config_dir}/rabbitmq.conf" "${rabbitmq_container}:/etc/rabbitmq/rabbitmq.conf"
docker start "${rabbitmq_container}" >/dev/null

ready="false"
for _attempt in $(seq 1 60); do
    if docker exec "${postgres_container}" pg_isready --quiet --username postgres && \
       docker exec "${rabbitmq_container}" rabbitmq-diagnostics -q ping && \
       docker exec "${rabbitmq_container}" rabbitmq-diagnostics -q check_port_listener 5672 >/dev/null 2>&1; then
        ready="true"
        break
    fi
    sleep 1
done
if [[ "${ready}" != "true" ]]; then
    docker logs "${rabbitmq_container}" >&2 || true
    docker logs "${postgres_container}" >&2 || true
    echo "Owned RabbitMQ/PostgreSQL fixtures did not become ready" >&2
    exit 1
fi

postgres_port="$(docker port "${postgres_container}" 5432/tcp)"
rabbitmq_port="$(docker port "${rabbitmq_container}" 5672/tcp)"
export TEST_DATABASE_URL="postgresql://postgres:integration-test@${postgres_port}/postgres"
export TEST_RABBITMQ_URL="amqp://guest:guest@${rabbitmq_port}/"

if ! uv run pytest -m "integration and not parity" tests/integration/test_write_admission_rabbitmq.py -q; then
    docker logs "${rabbitmq_container}" >&2 || true
    exit 1
fi
