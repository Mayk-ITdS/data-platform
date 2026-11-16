#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/../.."
# Unique namespace: never touches existing fema-cdc stacks or volumes.
export COMPOSE_PROJECT_NAME="fematest$(date +%s)$RANDOM"
COMPOSE_FILE=infrastructure/testing/compose.test.yml
mkdir -p artifacts/test-reports
# Report files remain after ephemeral containers and volumes are removed.
cleanup() {
  code=$?
  trap - EXIT INT TERM
  docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT_NAME" down --volumes --remove-orphans || true
  exit "$code"
}
trap cleanup EXIT INT TERM
# `run --rm` exits nonzero when the test command fails; reports are bind-mounted outside the removed volumes.
docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT_NAME" run --rm unit
docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT_NAME" run --rm spark-smoke
