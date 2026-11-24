"""Exercise the production transformations with one CDC envelope per configured topic."""
import base64
from datetime import datetime, timezone
import json
import runpy
from pathlib import Path

from pyspark.sql import SparkSession
from data_platform.demo.demo_cdc_generator import ORACLE_TABLES, SQL_TABLES
from data_platform.spark.bronze_ingestion import TOPICS
from data_platform.spark.canonical_silver_extensions import EXTENSIONS


def test_all_source_tables_reach_gold(tmp_path, monkeypatch):
    warehouse = str(tmp_path / "warehouse")
    monkeypatch.setenv("FEMA_WAREHOUSE", warehouse)
    def session():
        return (SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", 2)
                .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.lakehouse.type", "hadoop")
                .config("spark.sql.catalog.lakehouse.warehouse", warehouse).getOrCreate())
    session()
    runpy.run_module("data_platform.tests.integration.bootstrap_bronze_e2e", run_name="__main__")
    spark = session()
    events = []
    for topic in TOPICS:
        table = topic.rsplit(".", 1)[1]
        oracle = topic.startswith("assureplus")
        key = (ORACLE_TABLES if oracle else SQL_TABLES)[table][0]
        encoded = base64.b64encode(b"\x02").decode()
        payload = {key: encoded if oracle else 1}
        if table in {"POLICY", "HOLDER", "PRIVATE_CLIENT"}:
            payload["HOLDER_CONTEXT"] = "oracle-holder"
        if table == "PRIVATE_CLIENT":
            payload["AB_NOM"] = "Alias"
        if table == "CLAIM":
            payload["POLICY_RECORD_ID"] = encoded
        if table in {"Claims", "PolicyUnit"}:
            payload["PolicyId"] = 1
        if table == "PolicyUnit":
            payload.update({fk: 1 for _, _, _, fk in EXTENSIONS})
        if table in {"PrivateClient", "Policy"}:
            payload["HolderId"] = 1
        if table == "Holder":
            payload["HolderContext"] = "sql-holder"
        fields = [{"field": k, "type": "string" if isinstance(v, str) else "int64"} for k, v in payload.items()]
        envelope = {"schema": {"fields": [{"field": "after", "fields": fields}]},
                    "payload": {"op": "r", "before": None, "after": payload}}
        now = datetime.now(timezone.utc)
        events.append(("ORACLE" if oracle else "SQLSERVER", topic, 0, 0, now, b"{}", json.dumps(envelope).encode(), now))
    bronze = "lakehouse.bronze.cdc_events"
    spark.createDataFrame(events, spark.table(bronze).schema).writeTo(bronze).append()
    assert spark.table(bronze).select("topic").distinct().count() == 25
    for job in ["schema_inventory", "canonical_policy_claim", "canonical_silver_extensions", "build_gold_marts"]:
        session()
        runpy.run_path(str(Path("data_platform/spark") / (job + ".py")), run_name="__main__")
    spark = session()
    try:
        actual = {(r.entity, r.source_system): r.record_count for r in spark.table("lakehouse.gold.entity_counts").collect()}
        assert len(actual) == 25 and set(actual.values()) == {1}
        assert actual["private_client", "ORACLE"] == 1
        assert spark.sql("SELECT last_name FROM lakehouse.silver.private_client WHERE source_system='ORACLE'").first()[0] == "Alias"
        assert spark.table("lakehouse.gold.cdc_topic_activity").count() == 25
    finally:
        spark.stop()
