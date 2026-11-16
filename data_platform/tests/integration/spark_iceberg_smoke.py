"""Isolated Spark+Iceberg contract smoke test."""
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from pyspark.sql import SparkSession, Window, functions as F

spark = (SparkSession.builder.appName('FEMA-Iceberg-Smoke')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lakehouse.type', 'hadoop')
    .config('spark.sql.catalog.lakehouse.warehouse', '/lakehouse/warehouse')
    .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
checks=[]
def check(name, fn):
    try:
        fn()
        checks.append({'test':name,'status':'PASS'})
        print('PASS',name,flush=True)
    except Exception as exc:
        checks.append({'test':name,'status':'FAIL','error':str(exc)})
        print('FAIL',name,str(exc),flush=True)
try:
    spark.sql('CREATE NAMESPACE IF NOT EXISTS lakehouse.silver')
    def write_tables():
        spark.createDataFrame([
            ('ORACLE:3039','ORACLE'),('SQLSERVER:2','SQLSERVER')],
            ['canonical_policy_key','source_system']).writeTo('lakehouse.silver.policy').using('iceberg').create()
        snap = spark.createDataFrame([
            ('ORACLE:3039','ORACLE',54893,'100.0000','r'),
            ('ORACLE:3039','ORACLE',54986,'200.0000','u'),
            ('ORACLE:3039','ORACLE',54987,'300.0000','u'),
            ('ORACLE:3039','ORACLE',54988,'348.0000','u'),
            ('SQLSERVER:2','SQLSERVER',4,'500.0000','r')],
            ['canonical_policy_key','source_system','source_offset','policy_cost','op'])
        w=Window.partitionBy('canonical_policy_key').orderBy(F.col('source_offset').desc())
        (snap.withColumn('rn',F.row_number().over(w)).filter('rn = 1 AND op <> \'d\'')
            .drop('rn','op').writeTo('lakehouse.silver.policy_terms').using('iceberg').create())
    check('iceberg_create_and_write',write_tables)

    def latest():
        rows=spark.sql("SELECT source_offset, policy_cost FROM lakehouse.silver.policy_terms WHERE canonical_policy_key='ORACLE:3039'").collect()
        assert len(rows)==1 and rows[0]['source_offset']==54988 and rows[0]['policy_cost']=='348.0000',rows
    check('latest_oracle_cdc_state',latest)

    def integrity():
        assert spark.sql('SELECT COUNT(*) FROM lakehouse.silver.policy_terms').first()[0]==2
        assert spark.sql("""SELECT COUNT(*) FROM lakehouse.silver.policy_terms t
            LEFT ANTI JOIN lakehouse.silver.policy p
            ON t.canonical_policy_key=p.canonical_policy_key""").first()[0]==0
    check('iceberg_referential_integrity',integrity)

finally:
    Path('/reports').mkdir(parents=True,exist_ok=True)
    Path('/reports/spark-contract.json').write_text(json.dumps(checks,indent=2))
    suite = ET.Element('testsuite', name='spark_iceberg_smoke', tests=str(len(checks)),
                       failures=str(sum(x['status'] == 'FAIL' for x in checks)))
    for item in checks:
        case = ET.SubElement(suite, 'testcase', name=item['test'], classname='fema.spark')
        if item['status'] == 'FAIL':
            ET.SubElement(case, 'failure', message=item['error']).text = item['error']
    ET.ElementTree(suite).write('/reports/spark-smoke-junit.xml', encoding='utf-8', xml_declaration=True)
    spark.stop()
sys.exit(1 if any(x['status'] == 'FAIL' for x in checks) else 0)
