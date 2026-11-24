"""Validate the standalone Spark jobs in a private Iceberg warehouse."""
import runpy
from pathlib import Path

from pyspark.sql import SparkSession


def test_portfolio_jobs(tmp_path, monkeypatch, capsys):
    warehouse = str(tmp_path / "wh")
    export = tmp_path / "exports"
    export.mkdir()
    monkeypatch.setenv("FEMA_WAREHOUSE", warehouse)
    monkeypatch.setenv("FEMA_EXPORT_DIR", str(export))
    monkeypatch.setenv("PUBLISH_IDENTITY", "YES")
    monkeypatch.setenv("EXPECTED_RECONCILED_PAIRS", "1")

    def session():
        return (SparkSession.builder.master("local[2]").appName("portfolio-contract")
                .config("spark.sql.shuffle.partitions", 2)
                .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.lakehouse.type", "hadoop")
                .config("spark.sql.catalog.lakehouse.warehouse", warehouse).getOrCreate())

    def job(name):
        session()
        runpy.run_path(str(Path("data_platform/spark") / (name + ".py")), run_name="__main__")

    def table(name, query):
        spark.sql(f"CREATE OR REPLACE TABLE lakehouse.{name} USING iceberg AS " + query)

    spark = session()
    for name in ("silver", "bronze", "silver_identity_work"):
        spark.sql("CREATE NAMESPACE lakehouse." + name)
    table("silver.holder", """SELECT * FROM VALUES ('SQLSERVER','s','7'),('ORACLE','o','o')
          AS t(source_system, holder_context, source_primary_key)""")
    table("silver_identity_work.private_client_candidate_v1", """SELECT *,
          'Ada Exemple' full_name, 'Ada' first_name, 'Exemple' last_name, 'NY' state,
          'NY' state_code, 'F' sex, 1980 birth_year, 'ada@example.org' email FROM lakehouse.silver.holder""")
    job("holder_identity")
    spark = session()
    identities = spark.table("lakehouse.silver_identity_work.holder_canonical_v1").collect()
    assert len(identities) == 1 and identities[0].source_system_count == 2
    links = spark.table("lakehouse.silver_identity_work.holder_source_link_v1")
    links.withColumnRenamed("holder_context", "source_holder_context").withColumnRenamed(
        "source_primary_key", "source_holder_id").writeTo("lakehouse.silver_identity_work.link_input").using("iceberg").create()
    spark.sql("DROP TABLE lakehouse.silver_identity_work.holder_source_link_v1")
    spark.sql("ALTER TABLE lakehouse.silver_identity_work.link_input RENAME TO lakehouse.silver_identity_work.holder_source_link_v1")
    table("silver.policy", """SELECT * FROM VALUES ('SQLSERVER:1','SQLSERVER','7','s'),('ORACLE:2','ORACLE','2','o')
          AS t(canonical_policy_key,source_system,holder_id,holder_context)""")
    job("policy_holder_link")
    spark = session()
    assert {r.canonical_holder_id for r in spark.table("lakehouse.silver_identity_work.policy_holder_link_v1").collect()} == {identities[0].canonical_holder_id}
    table("silver_identity_work.policy_enriched_v1", "SELECT * FROM lakehouse.silver.policy")
    for name in ("premium_financial","building","risks","policy_terms","property","property_usage","policy_rating","holder_classification"):
        table("silver." + name, "SELECT canonical_policy_key, 42 AS amount FROM lakehouse.silver.policy")
    table("silver.claim", """SELECT 'ORACLE:3' canonical_claim_key, 'ORACLE:2' canonical_policy_key,
          'ORACLE' source_system, DATE '2021-01-01' date_of_loss""")
    job("export_powerbi")
    job("export_claims_powerbi")
    spark = session()
    portfolio = spark.read.parquet(str(export / "policy_portfolio.parquet"))
    assert {(r.canonical_policy_key, r.premium_financial__amount) for r in portfolio.collect()} == {("SQLSERVER:1",42),("ORACLE:2",42)}
    assert spark.read.parquet(str(export / "claims_bi.parquet")).first().canonical_claim_key == "ORACLE:3"
    table("bronze.cdc_events", """SELECT 'ORACLE' source_system, concat('assureplus_v2.ASSUREPLUS.',name) topic,
          0 partition, 10000L offset, current_timestamp() kafka_timestamp, current_timestamp() ingested_at,
          CAST('{"payload":{"POLICY_RECORD_ID":"Ag=="}}' AS BINARY) key_raw,
          CAST('{"payload":{"op":"r","after":{"HOLDER_CONTEXT":"o","AP_EMAIL":"ada@example.org"}}}' AS BINARY) value_raw
          FROM VALUES ('POLICY'),('PRIVATE_CLIENT') AS t(name)""")
    table("silver.source_schema_inventory", """SELECT 'assureplus_v2.ASSUREPLUS.PRIVATE_CLIENT' topic,
          'AP_EMAIL' source_column,'email' normalized_column,'string' connect_type,'string' logical_type""")
    table("silver.private_client", "SELECT 'ORACLE' source_system, 'ada@example.org' email")
    job("oracle_policy_silver")
    spark = session()
    assert spark.table("lakehouse.silver.oracle_policy").first().policy_record_id == 2
    job("audit_private_client_schema")
    job("verify_bronze")
    output = capsys.readouterr().out
    assert "Non-null Oracle emails: 1" in output and "BRONZE_EVENT_COUNT=1" in output
