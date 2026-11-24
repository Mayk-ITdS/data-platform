#!/usr/bin/env bash
# Temporary direct SQL bridge to existing Iceberg HadoopCatalog for Studi video.
# NO CSV and NO alternative Iceberg catalog/metastore are created.
# Power BI Desktop -> Spark SQL connector -> Spark Thrift -> Iceberg Gold.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${FEMA_SPARK_IMAGE:-apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu}"
PORT="${FEMA_THRIFT_PORT:-10000}"
mkdir -p "$ROOT/infrastructure/volumes/lakehouse" "$HOME/.ivy2"
if docker ps -a --format '{{.Names}}' | grep -qx 'fema-powerbi-sql'; then
  echo "Existing fema-powerbi-sql container found. Inspect with docker logs fema-powerbi-sql"
  exit 1
fi
docker run -d --name fema-powerbi-sql --restart unless-stopped \
  -p "127.0.0.1:${PORT}:10000" \
  -v "$ROOT/infrastructure/volumes/lakehouse:/lakehouse:ro" \
  -v "$HOME/.ivy2:/tmp/.ivy2" \
  --memory=3g \
  "$IMAGE" \
  /opt/spark/bin/spark-submit \
    --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
    --master 'local[2]' --driver-memory 2g \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.lakehouse.type=hadoop \
    --conf spark.sql.catalog.lakehouse.warehouse=/lakehouse/warehouse \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.sql.hive.thriftServer.singleSession=true \
    --conf spark.sql.hive.thriftServer.async=true \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.sql.warehouse.dir=/tmp/spark-sql-warehouse \
    --conf hive.server2.thrift.port=10000 \
    --conf hive.server2.thrift.bind.host=0.0.0.0 \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0 \
    /opt/spark/jars/spark-hive-thriftserver_2.13-4.1.0.jar

echo "SQL bridge container started. CHECK logs immediately:"
echo "  docker logs -f --tail 80 fema-powerbi-sql"
echo "Power BI Spark connector address: localhost:${PORT}"
echo "Run Gold FIRST (bash scripts/run_demo_pipeline.sh --once)."
