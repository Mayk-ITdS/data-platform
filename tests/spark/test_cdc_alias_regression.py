"""Regression tests for Oracle and SQL Server CDC normalization."""

import json
from datetime import datetime
from unittest.mock import patch

from pyspark.sql import functions as F

import data_platform.spark.canonical_silver_extensions as silver


ORACLE_TOPIC = "assureplus_v2.ASSUREPLUS.PRIVATE_CLIENT"
SQL_TOPIC = "abassurance.assurances.dbo.PrivateClient"


def metadata(source_column, normalized_column):
    return {
        "source_column": source_column,
        "normalized_column": normalized_column,
        "logical_type": None,
        "connect_type": "string",
        "decimal_scale": None,
        "decimal_precision": None,
    }


def source_row(topic, values):
    return {
        "_pk": "1",
        "topic": topic,
        "partition": 0,
        "offset": 1,
        "kafka_timestamp": datetime(2026, 11, 24),
        "op": "u",
        "record_json": json.dumps(values),
    }


def run_test():
    spark = silver.spark

    inventory = {
        ORACLE_TOPIC: [
            metadata("AB_PRENOM", "ab_prenom"),
            metadata("AB_NOM", "ab_nom"),
            metadata("AB_ADDRESS", "ab_address"),
            metadata("AB_EMAIL", "ab_email"),
        ],
        SQL_TOPIC: [
            metadata("Email", "email"),
        ],
    }

    oracle_cases = [
        (
            "Oracle historical AB_*",
            {
                "AB_PRENOM": "Marie",
                "AB_NOM": "Martin",
                "AB_ADDRESS": "Paris",
                "AB_EMAIL": "marie@example.fr",
            },
            "marie@example.fr",
        ),
        (
            "Oracle current AP_*",
            {
                "AP_PRENOM": "Marie",
                "AP_NOM": "Martin",
                "AP_ADDRESS": "Paris",
                "AP_EMAIL": "marie.new@example.fr",
            },
            "marie.new@example.fr",
        ),
        (
            "Oracle AP_* priority",
            {
                "AB_EMAIL": "obsolete@example.fr",
                "AP_EMAIL": "current@example.fr",
            },
            "current@example.fr",
        ),
    ]

    with patch.object(silver, "META", inventory):
        for label, payload, expected_email in oracle_cases:
            raw = spark.createDataFrame(
                [source_row(ORACLE_TOPIC, payload)]
            )

            with patch.object(silver, "latest_raw", return_value=raw):
                result = silver.extract(
                    "ORACLE",
                    "PRIVATE_CLIENT",
                    "HOLDER_CONTEXT",
                ).collect()

            assert len(result) == 1
            assert result[0]["email"] == expected_email, (
                f"{label}: wrong email"
            )
            print(f"PASS {label}", flush=True)

        sql_raw = spark.createDataFrame([
            source_row(
                SQL_TOPIC,
                {"Email": "sqlserver@example.fr"},
            )
        ])

        with patch.object(silver, "latest_raw", return_value=sql_raw):
            sql = silver.extract(
                "SQLSERVER",
                "PrivateClient",
                "HolderId",
            ).collect()

        assert len(sql) == 1
        assert sql[0]["email"] == "sqlserver@example.fr"
        print("PASS SQL Server Email", flush=True)

    print(
        "PASS: ORACLE + SQL SERVER CDC REGRESSION",
        flush=True,
    )


if __name__ == "__main__":
    try:
        run_test()
    finally:
        silver.spark.stop()
