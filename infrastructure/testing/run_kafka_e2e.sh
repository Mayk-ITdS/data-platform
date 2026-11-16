#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/../.."
COMPOSE_FILE=infrastructure/testing/compose.kafka.test.yml
export COMPOSE_PROJECT_NAME="femakafka$(date +%s)$RANDOM"
mkdir -p artifacts/test-reports
cleanup() {
  status=$?
  trap - EXIT INT TERM
  docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT_NAME" down --volumes --remove-orphans || true
  exit "$status"
}
trap cleanup EXIT INT TERM

dc() { docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT_NAME" "$@"; }

# Always use project-scoped isolated service names, network and disposable volume.
dc up -d --wait kafka
dc run --rm producer

SPARK_ARGS=(
  --master 'local[2]'
  --driver-memory 1536m
  --conf spark.sql.shuffle.partitions=2
  --conf spark.ui.enabled=false
  --conf spark.jars.ivy=/lakehouse/ivy-cache
  --packages 'org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0'
)

# The initial isolated table is a prerequisite of the production Bronze consumer.
dc run --rm --no-deps spark "${SPARK_ARGS[@]}" data_platform/tests/integration/bootstrap_bronze_e2e.py
# Actual production Spark jobs, unchanged.
dc run --rm --no-deps spark "${SPARK_ARGS[@]}" data_platform/spark/bronze_ingestion.py
dc run --rm --no-deps spark "${SPARK_ARGS[@]}" data_platform/spark/canonical_policy_claim.py
# Assertions and JUnit evidence from an isolated warehouse.
dc run --rm --no-deps spark "${SPARK_ARGS[@]}" data_platform/tests/integration/verify_kafka_e2e.py
