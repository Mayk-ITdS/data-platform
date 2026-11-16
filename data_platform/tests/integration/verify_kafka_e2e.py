"""Read-only verification against isolated outputs of actual Kafka -> Bronze -> Silver jobs."""
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName('FEMA-Kafka-E2E-Verify')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lakehouse.type', 'hadoop')
    .config('spark.sql.catalog.lakehouse.warehouse', '/lakehouse/warehouse')
    .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
results = []

def check(name, query, expected):
    try:
        actual = spark.sql(query).first()[0]
        if actual != expected:
            raise AssertionError(f'expected={expected!r}, actual={actual!r}')
        results.append({'name': name, 'status': 'PASS', 'actual': actual})
        print('PASS', name, flush=True)
    except Exception as exc:
        results.append({'name': name, 'status': 'FAIL', 'error': str(exc)})
        print('FAIL', name, str(exc), flush=True)

try:
    check('bronze_10_events', 'SELECT COUNT(*) FROM lakehouse.bronze.cdc_events', 10)
    check('bronze_kafka_tombstone', 'SELECT COUNT(*) FROM lakehouse.bronze.cdc_events WHERE value_raw IS NULL', 1)
    check('bronze_kafka_identity', '''SELECT COUNT(*) FROM (
        SELECT topic, partition, offset, COUNT(*) c FROM lakehouse.bronze.cdc_events
        GROUP BY topic, partition, offset HAVING COUNT(*) > 1) t''', 0)
    check('silver_2_policies', 'SELECT COUNT(*) FROM lakehouse.silver.policy', 2)
    check('silver_deleted_sql_policy', "SELECT COUNT(*) FROM lakehouse.silver.policy WHERE canonical_policy_key='SQLSERVER:2'", 0)
    check('silver_oracle_latest_update', "SELECT COUNT(*) FROM lakehouse.silver.policy WHERE canonical_policy_key='ORACLE:3039' AND source_offset=3", 1)
    check('silver_2_claims', 'SELECT COUNT(*) FROM lakehouse.silver.claim', 2)
    check('silver_claim_fk', '''SELECT COUNT(*) FROM lakehouse.silver.claim c
        LEFT ANTI JOIN lakehouse.silver.policy p ON c.canonical_policy_key=p.canonical_policy_key''', 0)
finally:
    spark.stop()
    out = Path('/reports')
    out.mkdir(parents=True, exist_ok=True)
    (out / 'kafka-e2e.json').write_text(json.dumps(results, indent=2))
    suite = ET.Element('testsuite', name='kafka_e2e', tests=str(len(results)),
                       failures=str(sum(r['status'] == 'FAIL' for r in results)))
    for r in results:
        case = ET.SubElement(suite, 'testcase', name=r['name'], classname='fema.kafka_e2e')
        if r['status'] == 'FAIL':
            ET.SubElement(case, 'failure', message=r['error']).text = r['error']
    ET.ElementTree(suite).write(out / 'kafka-e2e-junit.xml', encoding='utf-8', xml_declaration=True)
sys.exit(1 if any(r['status'] == 'FAIL' for r in results) else 0)
