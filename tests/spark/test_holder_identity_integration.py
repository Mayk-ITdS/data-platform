"""Read-only integration test of real Oracle/SQL Server identities."""

from pyspark.sql import functions as F
from data_platform.spark.holder_identity import (
    build_identity,
    create_spark,
)

EXPECTED_LINKS = 20_000
EXPECTED_CANONICAL = 19_997
EXPECTED_CROSS_SYSTEM = 3


def main():
    spark = create_spark()
    try:
        holders = spark.table("lakehouse.silver.holder")
        clients = spark.table(
            "lakehouse.silver_identity_work.private_client_candidate_v1"
        )

        canonical, linked = build_identity(holders, clients)

        metrics = {
            "source_links": linked.count(),
            "canonical_identities": canonical.count(),
            "cross_system_identities": canonical.where(
                F.col("source_system_count") == 2
            ).count(),
            "duplicate_links": (
                linked.count()
                - linked.select(
                    "source_system", "holder_context"
                ).distinct().count()
            ),
        }

        expected = {
            "source_links": EXPECTED_LINKS,
            "canonical_identities": EXPECTED_CANONICAL,
            "cross_system_identities": EXPECTED_CROSS_SYSTEM,
            "duplicate_links": 0,
        }

        for name, expected_value in expected.items():
            actual = metrics[name]
            if actual != expected_value:
                raise AssertionError(
                    f"{name}: expected {expected_value}, got {actual}"
                )
            print(f"PASS {name}: {actual}", flush=True)

        print("PASS: IDENTITY INTEGRATION — READ ONLY", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
