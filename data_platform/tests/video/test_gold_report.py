import base64
import json
import re
import runpy

import pytest
from pyspark.sql import SparkSession
from scripts import build_video_report as report

ENTITIES = "building claim holder holder_classification policy policy_rating policy_terms policy_unit premium_financial private_client property property_usage risks".split()


def test_iceberg_gold_to_power_bi(tmp_path, monkeypatch):
    warehouse = str(tmp_path / "warehouse")
    monkeypatch.setenv("FEMA_WAREHOUSE", warehouse)
    monkeypatch.setattr(report, "ROOT", tmp_path / "FEMA")

    def session():
        return (SparkSession.builder.master("local[2]")
                .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.lakehouse.type", "hadoop")
                .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
                .config("spark.sql.shuffle.partitions", 2).getOrCreate())

    spark = session()
    for ns in ("bronze", "silver"):
        spark.sql("CREATE NAMESPACE lakehouse." + ns)
    for name in ENTITIES:
        spark.sql(f"CREATE TABLE lakehouse.silver.{name} USING iceberg AS SELECT * FROM VALUES ('ORACLE'), ('SQLSERVER') AS t(source_system)")
    spark.sql("""CREATE TABLE lakehouse.bronze.cdc_events USING iceberg AS SELECT 'ORACLE' source_system,
                 'test' topic, current_timestamp() kafka_timestamp, current_timestamp() ingested_at""")
    runpy.run_module("data_platform.spark.build_gold_marts", run_name="__main__")
    spark = session()
    try:
        monkeypatch.setenv("FEMA_REPORT_DIR", str(report.ROOT))
        runpy.run_path("scripts/build_video_report.py", run_name="__main__")
        spark = session()
        model = json.loads((report.ROOT / "FEMA.SemanticModel/model.bim").read_text())
        tables = model["model"]["tables"]
        assert [t["name"] for t in tables] == ["entity_counts", "cdc_topic_activity"]
        snapshots = []
        for table in tables:
            expression = table["partitions"][0]["source"]["expression"]
            snapshots.append(json.loads(base64.b64decode(re.search(r'FromText\("([^"]+)"', expression)[1])))
        assert snapshots[0] == sorted([[e, s, 1] for e in ENTITIES for s in ["ORACLE", "SQLSERVER"]])
        assert snapshots[1] == [["test", "ORACLE", 1]]
        project = json.loads((report.ROOT / "FEMA.pbip").read_text())
        folder = report.ROOT / project["artifacts"][0]["report"]["path"]
        assert (folder / "definition.pbir").is_file()
        assert len(list(folder.rglob("visual.json"))) == 2
        spark.sql("DELETE FROM lakehouse.gold.entity_counts")
        with pytest.raises(ValueError, match="Empty Gold"):
            report.build(spark)
    finally:
        spark.stop()


@pytest.mark.parametrize("exists,columns,error", [
    (False, [], "Required Silver table missing"),
    (True, ["id"], "source_system column missing"),
    (True, ["source_system"], "Silver entity summary is empty"),
])
def test_gold_rejects_incomplete_inputs(monkeypatch, exists, columns, error):
    from unittest.mock import MagicMock
    spark = MagicMock()
    spark.catalog.tableExists.return_value = exists
    spark.table.return_value.columns = columns
    spark.table.return_value.groupBy.return_value.count.return_value.collect.return_value = []
    monkeypatch.setattr(SparkSession.Builder, "getOrCreate", lambda self: spark)
    with pytest.raises(RuntimeError, match=error):
        runpy.run_module("data_platform.spark.build_gold_marts", run_name="__main__")
    spark.stop.assert_called_once()
    spark.table.return_value.writeTo.assert_not_called()
