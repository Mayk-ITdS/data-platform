#!/usr/bin/env bash
# Sequential, bounded-resource demo pipeline. Run from the repository root.
# USE: bash scripts/run_demo_pipeline.sh --once
#      bash scripts/run_demo_pipeline.sh --loop 60
# Batch Silver is rebuilt each cycle; it is NOT a 2-second streaming Silver job.
set -Eeuo pipefail

MODE="${1:---once}"
INTERVAL="${2:-60}"
if [[ "$MODE" != "--once" && "$MODE" != "--loop" ]]; then
  echo "Usage: bash scripts/run_demo_pipeline.sh [--once | --loop SECONDS]" >&2
  exit 2
fi
if [[ "$MODE" == "--loop" && ! "$INTERVAL" =~ ^[0-9]+$ ]]; then
  echo "INTERVAL must be an integer number of seconds" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p infrastructure/volumes/lakehouse
mkdir -p "${HOME}/.ivy2"
mkdir -p /tmp/fema-demo
exec 9>/tmp/fema-demo/pipeline.lock
flock -n 9 || { echo "Another demo pipeline already runs." >&2; exit 1; }

IMAGE="${FEMA_SPARK_IMAGE:-apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu}"
NETWORK="${FEMA_DOCKER_NETWORK:-$(docker inspect kafka-fema --format '{{range $name, $net := .NetworkSettings.Networks}}{{println $name}}{{end}}' | head -n 1)}"
if [[ -z "$NETWORK" ]]; then echo "Cannot discover Kafka docker network" >&2; exit 1; fi

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"
spark_job() {
  local script="$1"
  shift
  echo "$(date -Is) RUNNING $script"
  docker run --rm --network "$NETWORK" \
    -v "$ROOT:/workspace:ro" \
    -v "$ROOT/infrastructure/volumes/lakehouse:/lakehouse" \
    -v "$HOME/.ivy2:/tmp/.ivy2" \
    -e KAFKA_BOOTSTRAP_SERVERS=kafka-fema:9092 \
    -e BRONZE_TRIGGER=availableNow \
    -e KAFKA_STARTING_OFFSETS=latest \
    "$IMAGE" \
    /opt/spark/bin/spark-submit \
      --master 'local[2]' \
      --driver-memory 3g \
      --conf spark.sql.shuffle.partitions=8 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --packages "$PACKAGES" \
      "/workspace/$script" "$@"
}

cycle() {
  spark_job data_platform/spark/bronze_ingestion.py
  spark_job data_platform/spark/canonical_policy_claim.py
  spark_job data_platform/spark/canonical_silver_extensions.py
  spark_job data_platform/spark/build_gold_marts.py
  echo "$(date -Is) PASS: Kafka -> Iceberg Bronze -> Silver -> Gold batch cycle"
}

if [[ "$MODE" == "--once" ]]; then
  cycle
else
  while true; do
    cycle
    echo "Next cycle in $INTERVAL seconds (Ctrl+C stops)"
    sleep "$INTERVAL"
  done
fi
