#!/usr/bin/env bash
# Studi video: Docker-hosted source services, ephemeral Spark jobs, persistent SQL bridge.
# The orchestration script runs on the WSL host; processing runs in Docker containers.
# No Kubernetes, CSV, or host-side Spark installations.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

for container in kafka-fema connect-fema sqlserver-fema oracle-fema-v2; do
  if ! docker ps --format '{{.Names}}' | grep -qx "$container"; then
    echo "ERROR: Required service container is not running: $container" >&2
    echo "Start it first (docker start $container) and inspect its logs." >&2
    exit 1
  fi
done

echo "Service containers present (not a CDC end-to-end health guarantee)."
echo "=== Step 1: Kafka -> Bronze -> Silver -> Gold (ephemeral Spark containers) ==="
bash scripts/run_demo_pipeline.sh --once

echo "=== Step 2: Start read-only SQL bridge container for Power BI ==="
if docker ps --format '{{.Names}}' | grep -qx fema-powerbi-sql; then
  echo "SQL bridge already running."
elif docker ps -a --format '{{.Names}}' | grep -qx fema-powerbi-sql; then
  echo "SQL bridge container exists but is stopped: inspect docker logs fema-powerbi-sql" >&2
  exit 1
else
  bash scripts/start_powerbi_sql.sh
fi

echo "=== Check the SQL bridge boot logs ==="
docker logs --tail 35 fema-powerbi-sql || true

cat <<'EOF'

Containers:
  - sqlserver-fema     (source DB)
  - oracle-fema-v2     (source DB)
  - kafka-fema         (broker)
  - connect-fema       (Debezium Connect)
  - ephemeral Spark containers for Bronze, Silver and Gold
  - fema-powerbi-sql   (SQL endpoint for existing Iceberg HadoopCatalog)

Power BI Desktop runs on Windows, not inside Docker.
Next, validate the SQL endpoint with a real SQL SELECT against
lakehouse.gold.entity_counts before using Power BI's Spark connector.
EOF
