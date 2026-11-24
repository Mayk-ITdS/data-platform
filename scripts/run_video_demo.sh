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

python3 scripts/check_video_sources.py
echo "=== Step 1: Kafka -> Bronze -> Silver -> Gold (ephemeral Spark containers) ==="
bash scripts/run_demo_pipeline.sh --once

report="$ROOT/infrastructure/volumes/lakehouse/video/FEMA/FEMA.pbip"
[[ -s "$report" ]]
if command -v wslpath >/dev/null && command -v explorer.exe >/dev/null; then
  echo "Ouverture du rapport Power BI Desktop"
  explorer.exe "$(wslpath -w "$report")" || [[ "$?" -eq 1 ]]
else
  echo "Ouvrir dans Power BI Desktop : $report"
fi
