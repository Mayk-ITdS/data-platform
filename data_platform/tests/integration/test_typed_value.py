"""Run with pytest and PySpark 4.1, independently of Kafka and Iceberg."""
import pytest
from pyspark.sql import SparkSession

from data_platform.spark.canonical_silver_extensions import typed_value


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("alias-regression").getOrCreate()
    yield session
    session.stop()


@pytest.mark.parametrize("source,payload,expected", [
    ("AB_AGE", '{"AP_AGE":39}', 39),
    ("AP_AGE", '{"AB_AGE":40}', 40),
    ("AB_AGE", '{"AB_AGE":38,"AP_AGE":39}', 39),
    ("AP_AGE", '{"AP_AGE":null,"AB_AGE":40}', 40),
    ("AGE", '{"AP_AGE":39}', None),
])
def test_typed_alias(spark, source, payload, expected):
    frame = spark.createDataFrame([(payload,)], ["record_json"])
    meta = {"source_column": source, "logical_type": None, "connect_type": "int32"}
    result = frame.select(typed_value(frame, meta).alias("age"))
    assert result.schema["age"].dataType.simpleString() == "bigint"
    assert result.first().age == expected


def test_debezium_logical_types(spark):
    from datetime import date
    from decimal import Decimal
    from data_platform.spark import canonical_silver_extensions as silver
    import json

    values = {"AP_AMOUNT": "BNI=", "DAY": 1, "MS": 2000, "US": 2000000,
              "FLAG": True, "RATIO": 1.25, "NAME": "FEMA"}
    data = spark.createDataFrame([(json.dumps(values),)], ["record_json"])
    def col(name, logical, connect="string"):
        return silver.typed_value(data, {"source_column": name, "logical_type": logical,
                                  "connect_type": connect, "decimal_scale": 2, "decimal_precision": 10})
    record = data.select(
        col("AB_AMOUNT", "org.apache.kafka.connect.data.Decimal").alias("amount"),
        col("DAY", "io.debezium.time.Date").alias("day"),
        col("MS", "io.debezium.time.Timestamp").cast("long").alias("seconds"),
        col("US", "io.debezium.time.MicroTimestamp").cast("long").alias("microseconds_as_seconds"),
        col("FLAG", None, "boolean").alias("flag"), col("RATIO", None, "float64").alias("ratio"),
        col("NAME", None).alias("name")).first()
    assert tuple(record) == (Decimal("12.34"), date(1970, 1, 2), 2, 2, True, 1.25, "FEMA")
    assert silver.decode_oracle_integer.func(None) is None
    assert silver.decode_oracle_integer.func("/w==") == "-1"
    with pytest.raises(ValueError, match="Invalid Oracle integer"):
        silver.decode_oracle_integer.func("abc")
    assert silver.decimal_udf(10, 2).func(None) is None
    assert silver.decimal_udf(10, 2).func("BNI=") == Decimal("12.34")
    assert silver.normalize_source_column("ab_nom", "ORACLE") == "ap_nom"
    assert silver.normalize_source_column("ab_nom", "SQLSERVER") == "ab_nom"


@pytest.mark.parametrize("left,right,expected", [
    ("true", "1", "boolean"), ("0", "false", "boolean"),
    ("DATE '2020-01-02'", "TIMESTAMP '2020-01-02 00:00:00'", "timestamp"),
    ("CAST(2.1 AS DECIMAL(5,1))", "CAST(2.10 AS DECIMAL(6,2))", "decimal(6,2)"),
])
def test_harmonize_types(spark, left, right, expected):
    from data_platform.spark.canonical_silver_extensions import harmonize
    a, b = harmonize(spark.sql("SELECT " + left + " AS val"), spark.sql("SELECT " + right + " AS val"))
    assert a.dtypes == b.dtypes == [("val", expected)]
    assert a.first()[0] == b.first()[0]


def test_reject_incompatible_and_missing_metadata(spark, monkeypatch):
    from data_platform.spark import canonical_silver_extensions as silver
    with pytest.raises(ValueError, match="type mismatch"):
        silver.harmonize(spark.sql("SELECT 'text' AS x"), spark.sql("SELECT 1 AS x"))
    monkeypatch.setattr(silver, "META", {}, raising=False)
    with pytest.raises(ValueError, match="absent"):
        silver.latest_raw("ORACLE", "UNKNOWN", "ID")
