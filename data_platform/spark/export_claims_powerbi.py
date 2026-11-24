from pathlib import Path
import shutil

import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("FEMA-Claims-PowerBI")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        os.getenv("FEMA_WAREHOUSE", "/lakehouse/warehouse")
    )
    .getOrCreate()
)

try:
    claims = spark.table("lakehouse.silver.claim").select(
        "canonical_claim_key",
        "canonical_policy_key",
        "source_system",
        "date_of_loss"
    )

    count = claims.count()

    if count == 0:
        raise RuntimeError("Claims Silver is empty")

    export_dir = Path(os.getenv("FEMA_EXPORT_DIR", "/exports"))
    output = export_dir / "claims_bi_tmp"
    destination = export_dir / "claims_bi.parquet"

    claims.coalesce(1).write.mode("overwrite").parquet(str(output))

    parts = list(output.glob("part-*.parquet"))
    if len(parts) != 1:
        raise RuntimeError("Expected exactly one Parquet file")

    parts[0].replace(destination)
    shutil.rmtree(output)

    print(f"PASS: {count} claims exported")

finally:
    spark.stop()
