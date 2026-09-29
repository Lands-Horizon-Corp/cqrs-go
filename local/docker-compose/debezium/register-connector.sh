#!/bin/sh
# Registers the Postgres source connector with Kafka Connect once it's
# reachable. Idempotent: if the connector already exists (HTTP 409), that
# counts as success rather than an error.
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONFIG_FILE="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/connector-postgres-write.json"
NAME="cqrs-write-db-connector"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
  sleep 2
done

echo "Registering connector '${NAME}' ..."
status=$(curl -s -o /tmp/register-connector.out -w "%{http_code}" \
  -X POST -H "Content-Type: application/json" \
  --data @"${CONFIG_FILE}" \
  "${CONNECT_URL}/connectors")

case "$status" in
  200|201)
    echo "Connector registered."
    ;;
  409)
    echo "Connector already registered, leaving it as-is."
    ;;
  *)
    echo "Unexpected response (HTTP ${status}):"
    cat /tmp/register-connector.out
    exit 1
    ;;
esac

echo
echo "Current connector status:"
curl -s "${CONNECT_URL}/connectors/${NAME}/status"
echo
