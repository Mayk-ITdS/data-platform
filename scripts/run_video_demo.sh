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

export DEMO_WIDTH="$(tput cols 2>/dev/null || echo 120)"

banner() {
    local message="$1"
    local padding=$(( (DEMO_WIDTH - ${#message}) / 2 ))
    (( padding < 0 )) && padding=0

    printf '\n%*s\033[1;36m%s\033[0m\n\n' \
        "$padding" "" "$message"
}

success() {
    local message="$1"
    local padding=$(( (DEMO_WIDTH - ${#message}) / 2 ))
    (( padding < 0 )) && padding=0

    printf '\n%*s\033[1;32m%s\033[0m\n\n' \
        "$padding" "" "$message"
}

banner "FEMA DATA PLATFORM — DEMONSTRATION END-TO-END"

banner "01 | VALIDATION DES SYSTEMES SOURCES"

python3 scripts/check_video_sources.py

success "PASS — VALIDATION DES SYSTEMES SOURCES"

sleep 5
banner "KAFKA → BRONZE → SILVER → GOLD → POWER BI"
bash scripts/run_demo_pipeline.sh --once

report="$ROOT/infrastructure/volumes/lakehouse/video/FEMA/FEMA.pbip"
[[ -s "$report" ]]
if command -v wslpath >/dev/null && command -v explorer.exe >/dev/null; then
  success "PASS — RAPPORT POWER BI GENERE"
  banner "OUVERTURE AUTOMATIQUE DE POWER BI DESKTOP"
  sleep 5
  explorer.exe "$(wslpath -w "$report")" || [[ "$?" -eq 1 ]]
else
  echo "Ouvrir dans Power BI Desktop : $report"
fi
