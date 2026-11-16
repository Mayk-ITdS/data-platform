"""Read-only checks against CURRENT production Bronze/Silver Iceberg.
Run with spark-submit; never creates/replaces any tables or writes warehouse files.
Produces JUnit XML and JSON in /reports, even when checks fail.
"""
import json
import os
import sys
import traceback
from pathlib import Path
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession

TABLES = ['policy_terms','building','risks','property','property_usage',
          'policy_rating','premium_financial','holder_classification']
OUT = Path(os.getenv('TEST_REPORT_DIR','/reports'))
OUT.mkdir(parents=True, exist_ok=True)
results=[]
spark=(SparkSession.builder.appName('FEMA-Silver-Read-Only-QA')
    .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lakehouse','org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lakehouse.type','hadoop')
    .config('spark.sql.catalog.lakehouse.warehouse','/lakehouse/warehouse')
    .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')

def check(name,query,expected=0):
    try:
        actual=spark.sql(query).first()[0]
        if actual != expected:
            raise AssertionError(f'expected {expected}, observed {actual}')
        results.append({'test':name,'status':'PASS','actual':actual})
        print('PASS',name,flush=True)
    except Exception as exc:
        results.append({'test':name,'status':'FAIL','reason':str(exc)})
        print('FAIL',name,str(exc),flush=True)

try:
    for table in TABLES:
        t=f'lakehouse.silver.{table}'
        check(table+':null_keys',f'SELECT COUNT(*) FROM {t} WHERE canonical_policy_key IS NULL')
        check(table+':duplicate_keys',f"""SELECT COUNT(*) FROM (
            SELECT canonical_policy_key FROM {t}
            GROUP BY canonical_policy_key HAVING COUNT(*) > 1) d""")
        check(table+':orphan_keys',f"""SELECT COUNT(*) FROM {t} e LEFT ANTI JOIN
            lakehouse.silver.policy p ON e.canonical_policy_key=p.canonical_policy_key""")
        check(table+':policy_set_mismatch',f"""SELECT COUNT(*) FROM lakehouse.silver.policy p
            LEFT ANTI JOIN {t} e ON p.canonical_policy_key=e.canonical_policy_key""")
    check('claims:orphan_policy', """SELECT COUNT(*) FROM lakehouse.silver.claim c
        LEFT ANTI JOIN lakehouse.silver.policy p ON c.canonical_policy_key=p.canonical_policy_key""")
    check('claims:duplicate_claim_key', """SELECT COUNT(*) FROM (
        SELECT canonical_claim_key FROM lakehouse.silver.claim
        GROUP BY canonical_claim_key HAVING COUNT(*)>1) d""")
    check('regression:oracle_3039_latest', """SELECT COUNT(*) FROM lakehouse.silver.policy_terms
        WHERE canonical_policy_key='ORACLE:3039'
          AND source_offset=54988 AND CAST(policy_cost AS DECIMAL(16,4))=348.0000""", expected=1)
    check('regression:oracle_3039_single', """SELECT COUNT(*) FROM lakehouse.silver.policy_terms
        WHERE canonical_policy_key='ORACLE:3039' """, expected=1)
finally:
    spark.stop()
    (OUT/'silver-integrity.json').write_text(json.dumps(results,indent=2))
    suite=ET.Element('testsuite',name='fema_silver_integrity',tests=str(len(results)),
                     failures=str(sum(r['status']=='FAIL' for r in results)))
    for item in results:
        case=ET.SubElement(suite,'testcase',name=item['test'],classname='fema.silver')
        if item['status']=='FAIL':
            ET.SubElement(case,'failure',message=item['reason']).text=item['reason']
    ET.ElementTree(suite).write(OUT/'silver-integrity-junit.xml', encoding='utf-8',xml_declaration=True)

sys.exit(1 if any(r['status']=='FAIL' for r in results) else 0)
