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
