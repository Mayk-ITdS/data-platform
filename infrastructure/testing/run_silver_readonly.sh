#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/../.."
mkdir -p artifacts/test-reports
# Warehouse mounted read-only! No production checkpoints or volumes removed.
docker run --rm --memory 3g --cpus 2 --user root \
 -e HOME=/tmp -e TEST_REPORT_DIR=/reports \
 -v "$PWD/data_platform:/workspace/data_platform:ro" \
 -v "$PWD/infrastructure/volumes/lakehouse:/lakehouse:ro" \
 -v "$PWD/artifacts/test-reports:/reports" \
 -v "fema_qa_ivy_cache:/tmp/ivy-cache" \
 -w /workspace --entrypoint /opt/spark/bin/spark-submit \
 apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu \
 --master 'local[2]' --driver-memory 1536m \
 --conf spark.sql.shuffle.partitions=4 \
 --conf spark.ui.enabled=false \
 --conf spark.jars.ivy=/tmp/ivy-cache \
 --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0 \
 data_platform/tests/integration/silver_integrity_readonly.py
