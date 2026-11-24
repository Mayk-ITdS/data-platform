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
catalog_type=hadoop
warehouse=/lakehouse/warehouse
catalog_args=()
container_env=()
if [[ -n "${FEMA_REST_URI:-}" ]]; then
  catalog_type=rest
  warehouse=s3://fema-lakehouse/warehouse
  PACKAGES+=,org.apache.iceberg:iceberg-aws-bundle:1.12.0
  catalog_args=(--conf "spark.sql.catalog.lakehouse.uri=$FEMA_REST_URI"
    --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO
    --conf "spark.sql.catalog.lakehouse.s3.endpoint=${FEMA_S3_ENDPOINT:?}"
    --conf spark.sql.catalog.lakehouse.s3.path-style-access=true)
  : "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}"
  container_env=(-v /etc/fema-rgw/client-cacerts:/etc/fema/client-cacerts:ro
    -e JAVA_TOOL_OPTIONS=-Djavax.net.ssl.trustStore=/etc/fema/client-cacerts --add-host ceph-rgw:host-gateway -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION=us-east-1)
fi
spark_job() {
  local script="$1"
  shift
  echo "$(date -Is) RUNNING $script"
  docker run --rm --network "$NETWORK" \
    --user "$(id -u):$(id -g)" -e SPARK_USER=fema -e HOME=/tmp \
    -v /etc/passwd:/etc/passwd:ro \
    -v /etc/group:/etc/group:ro \
    -v "$ROOT:/workspace:ro" \
    -v "$ROOT/infrastructure/volumes/lakehouse:/lakehouse" \
    -v "$HOME/.ivy2:/tmp/.ivy2" \
    -e FEMA_CATALOG_TYPE="$catalog_type" -e FEMA_WAREHOUSE="$warehouse" \
    "${container_env[@]}" \
    -e KAFKA_BOOTSTRAP_SERVERS=kafka-fema:9092 \
    -e BRONZE_CHECKPOINT="/lakehouse/checkpoints/${FEMA_CHECKPOINT_NAME:-bronze_unified_v1}" \
    -e BRONZE_TRIGGER=availableNow \
    -e KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-latest}" \
    "$IMAGE" \
    /opt/spark/bin/spark-submit \
      --master 'local[2]' \
      --driver-memory 3g \
      --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
      --conf "spark.sql.catalog.lakehouse.type=$catalog_type" \
      --conf "spark.sql.catalog.lakehouse.warehouse=$warehouse" \
      "${catalog_args[@]}" \
      --conf spark.sql.shuffle.partitions=8 \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --repositories https://repo.maven.apache.org/maven2 \
      --packages "$PACKAGES" \
      "/workspace/$script" "$@"
}

GREEN=$'\033[1;32m'
CYAN=$'\033[1;36m'
RED=$'\033[1;31m'
RESET=$'\033[0m'

DEMO_PAUSE="${FEMA_DEMO_PAUSE:-5}"

export DEMO_WIDTH="$(tput cols 2>/dev/null || echo 120)"
export PYTHONUNBUFFERED=1

center_output() {
    python3 -u -c '
import os
import sys

width = int(os.environ.get("DEMO_WIDTH", "120"))
padding = " " * max(0, (width - 100) // 2)

for line in sys.stdin:
    sys.stdout.write(padding + line)
    sys.stdout.flush()
'
}

exec > >(center_output) 2>&1

export COLUMNS=100

center() {
    local message="$1"
    local color="${2:-$RESET}"
    local width
    width=100
    local padding=$(( (width - ${#message}) / 2 ))

    if (( padding < 0 )); then
        padding=0
    fi

    printf '%*s%s%s%s\n' "$padding" "" "$color" "$message" "$RESET"
}

stage() {
    local title="$1"
    local pause="$2"
    shift 2

    local started=$SECONDS
    local status=0

    printf '\n'
    center "====================================================================" "$CYAN"
    center "$title" "$CYAN"
    center "====================================================================" "$CYAN"
    printf '\n'

    "$@" || status=$?

    printf '\n'

    if (( status != 0 )); then
        center "FAIL — $title (code $status)" "$RED"
        return "$status"
    fi

    center "PASS — $title — $((SECONDS - started)) s" "$GREEN"

    printf '\n'
    sleep "${FEMA_DEMO_PAUSE_OVERRIDE:-$pause}"
}


publish_gold() {
    local branch="feature/gold-business-analytics"
    local repo="Mayk-ITdS/data-platform"
    local pr

    [[ "$(git branch --show-current)" == "$branch" ]] || return 1

    gh auth status --hostname github.com || return 1

    center "Branche source : $branch" "$CYAN"
    center "Branche cible : develop" "$CYAN"

    git push origin "HEAD:refs/heads/$branch" || return 1

    center "PASS - Code source publie sur GitHub" "$GREEN"

    pr="$(gh pr list \
        --repo "$repo" \
        --head "$branch" \
        --base develop \
        --state open \
        --json url \
        --jq '.[0].url // empty')" || return 1

    if [[ -z "$pr" ]]; then
        pr="$(gh pr create \
            --repo "$repo" \
            --head "$branch" \
            --base develop \
            --title "Gold business analytics and Power BI reporting" \
            --body "Construction des Gold Marts et demonstration end-to-end du pipeline FEMA." \
        )" || return 1
    fi

    center "PASS - Pull Request vers develop" "$GREEN"
    center "$pr" "$GREEN"
    center "Fusion en attente de validation CI/CD" "$CYAN"
}

cycle() {
    stage "02 | INITIALISATION BRONZE" 5 \
        spark_job data_platform/tests/integration/bootstrap_bronze_e2e.py

    stage "03 | INGESTION DES EVENEMENTS CDC" 5 \
        spark_job data_platform/spark/bronze_ingestion.py

    stage "04 | INVENTAIRE DES SCHEMAS" 8 \
        spark_job data_platform/spark/schema_inventory.py

    stage "05 | INTEGRATION DES CONTRATS ET SINISTRES" 8 \
        spark_job data_platform/spark/canonical_policy_claim.py

    stage "06 | HARMONISATION DES ENTITES SILVER" 8 \
        spark_job data_platform/spark/canonical_silver_extensions.py

    stage "07 | CONSTRUCTION DES GOLD MARTS" 15 \
        spark_job data_platform/spark/build_gold_marts.py

    if [[ "${FEMA_DEMO_PUBLISH:-0}" == "1" ]]; then
        stage "08 | PUBLICATION GITHUB" 8 publish_gold
    fi
    stage "09 | GENERATION DU RAPPORT POWER BI" 5 \
        spark_job scripts/build_video_report.py

    center "PIPELINE TERMINE AVEC SUCCES" "$GREEN"
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
