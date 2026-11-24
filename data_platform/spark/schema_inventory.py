import json
import re
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
import time

spark = (
    SparkSession.builder
    .appName("FEMA-Schema-Inventory")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.lakehouse.type", os.getenv("FEMA_CATALOG_TYPE", "hadoop"))
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        os.getenv("FEMA_WAREHOUSE", "/lakehouse/warehouse")
    )
    .getOrCreate()
)

def snake_case(name: str) -> str:
    """Convert CamelCase and UPPER_CASE to snake_case."""
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()

bronze = spark.table("lakehouse.bronze.cdc_events")

schemas = (
    bronze
    .where("value_raw IS NOT NULL")
    .select("source_system", "topic", "offset", "value_raw")
    .groupBy("source_system", "topic")
    .agg(
        F.max_by(
            "value_raw", "offset"
        ).alias("value_raw")
    )
    .collect()
)

rows = []

for record in schemas:
    envelope = json.loads(bytes(record.value_raw).decode("utf-8"))

    fields = envelope["schema"]["fields"]

    after = next(
        (field for field in fields if field.get("field") == "after"),
        None
    )

    if after is None:
        raise ValueError(f"No after schema: {record.topic}")

    for field in after.get("fields", []):
        parameters = field.get("parameters") or {}

        rows.append((
            record.source_system,
            record.topic,
            field["field"],
            snake_case(field["field"]),
            field["type"],
            field.get("name"),
            parameters.get("scale"),
            parameters.get("connect.decimal.precision"),
            field.get("optional", True),
        ))


schema = T.StructType([
    T.StructField("source_system", T.StringType()),
    T.StructField("topic", T.StringType()),
    T.StructField("source_column", T.StringType()),
    T.StructField("normalized_column", T.StringType()),
    T.StructField("connect_type", T.StringType()),
    T.StructField("logical_type", T.StringType()),
    T.StructField("decimal_scale", T.StringType()),
    T.StructField("decimal_precision", T.StringType()),
    T.StructField("nullable", T.BooleanType()),
])

inventory = spark.createDataFrame(rows, schema=schema)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

(
    inventory.writeTo("lakehouse.silver.source_schema_inventory")
    .using("iceberg")
    .createOrReplace()
)

(
    inventory
    .groupBy("source_system","topic")
    .agg(F.count('*').alias('columns_count'))
    .orderBy('source_system','topic')
    .show(50, truncate=False)
)

print(f"\nTotal source tables inventoried: {len(schemas)}\n")
print(f"\nTotal source columns inventoried: {len(rows)}\n")
print("\nPASS: source schema inventory\n",flush=True) # flush buffer before print ( for reading )

time.sleep(5) # to be able to read a little

spark.stop()