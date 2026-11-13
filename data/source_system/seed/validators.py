from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_holders(df: DataFrame) -> None:
    df.select(
        F.count("*").alias("total"),

        F.sum(
            F.col("HolderContext").isNull().cast("long")
        ).alias("null_context"),

        F.sum(
            F.col("HolderType").isNull().cast("long")
        ).alias("null_type"),

        F.sum(
            (
                    F.col("HolderContext").isNotNull()
                    & (F.length("HolderContext") != 64)
            ).cast("long")
        ).alias("invalid_hash_length"),

    ).show()
    invalid = df.filter(
        F.col("HolderContext").isNull()
        | F.col("HolderType").isNull()
        | (F.length("HolderContext") != 64)
    )

    if invalid.limit(1).count() > 0:
        raise ValueError("Invalid holder records")

    conflicts = (
        df.groupBy("HolderContext")
        .agg(
            F.countDistinct("HolderType")
            .alias("type_count")
        )
        .filter(F.col("type_count") > 1)
    )

    if conflicts.limit(1).count() > 0:
        raise ValueError("Conflicting holder types")

def validate_private_clients(df: DataFrame) -> None:

    invalid = df.filter(
        F.col("HolderContext").isNull()
        | (F.length("HolderContext") != 64)
        | F.col("State").isNull()
        | (F.trim("State") == "")
    )

    if invalid.limit(1).count() > 0:
        raise ValueError("Invalid PrivateClient required fields")

    duplicates = (
        df.groupBy("HolderContext")
        .count()
        .filter(F.col("count") > 1)
    )

    if duplicates.limit(1).count() > 0:
        raise ValueError("Duplicate PrivateClient HolderContext")

    invalid_values = df.filter(
        (F.length("FullName") > 150)
        | (F.length("Email") > 254)
        | (F.length("Address") > 255)
        | (F.length("State") > 12)
        | (
            F.col("Sex").isNotNull()
            & ~F.col("Sex").isin("F", "M")
        )
        | (
            F.col("BirthYear").isNotNull()
            & ~F.col("BirthYear").between(0, 9999)
        )
    )

    if invalid_values.limit(1).count() > 0:
        raise ValueError("PrivateClient exceeds SQL schema constraints")
