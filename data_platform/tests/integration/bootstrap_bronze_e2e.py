"""Create the real Bronze table schema in an isolated, empty Iceberg catalog."""
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName('FEMA-E2E-Bronze-Bootstrap')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lakehouse.type', 'hadoop')
    .config('spark.sql.catalog.lakehouse.warehouse', '/lakehouse/warehouse')
    .getOrCreate())
try:
    spark.sql('CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze')
    spark.sql('''CREATE TABLE IF NOT EXISTS lakehouse.bronze.cdc_events (
        source_system STRING,
        topic STRING,
        partition INT,
        offset BIGINT,
        kafka_timestamp TIMESTAMP,
        key_raw BINARY,
        value_raw BINARY,
        ingested_at TIMESTAMP
    ) USING iceberg''')
    print('PASS isolated_bronze_bootstrap', flush=True)
finally:
    spark.stop()
