from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def extract_holders(df: DataFrame) -> DataFrame:
    return (
        df.filter((F.col("holder_type").isNotNull())&
                  F.col("AB_ADRE")
                  )
        .select("holder_context", "holder_type")
        .dropDuplicates(["holder_context"])
    )

def extract_oracle_holders(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(
            F.col("holder_type").isNotNull()
            & F.col("holder_context").isNotNull()
            & (
                (F.col("holder_type") != "PERSON")
                | (
                    F.col("AB_PRENOM").isNotNull()
                    & (F.trim(F.col("AB_PRENOM")) != "")
                    & F.col("AB_NOM").isNotNull()
                    & (F.trim(F.col("AB_NOM")) != "")
                    & F.col("AB_ADDRESS").isNotNull()
                    & (F.trim(F.col("AB_ADDRESS")) != "")
                )
            )
        )
        .select("holder_context", "holder_type")
        .dropDuplicates()
    )

def extract_struct(
    df: DataFrame,
    struct_name: str,
    mapping:Dict,
    holder_keys
) -> DataFrame:
    return (df.join(
        F.broadcast(holder_keys),
        on='holder_context',
        how='left_semi'
    )
    .select(
        F.col('unit_properties.policy_record_id').alias('policy_record_id'),
        *[
            F.col(f'unit_properties.{struct_name}.{source}').alias(target)
            for source, target in mapping.items()
        ]
    ))

def extract_private_clients(df,holders:DataFrame):

    holder_keys = (holders.select(F.col('HolderContext').alias('holder_context')).distinct())

    return (
        df
        .filter(F.col("holder_type") == "PERSON")
        .join(F.broadcast(holder_keys), on='holder_context', how='left_semi')
        .select(
            F.col("holder_context").alias("HolderContext"),
            F.concat_ws(" ", "AB_PRENOM", "AB_NOM").alias("FullName"),
            F.col("birth_year").cast("smallint").alias("BirthYear"),
            F.col("sex").alias("Sex"),
            F.col("AB_EMAIL").alias("Email"),
            F.col("AB_ADDRESS").alias("Address"),
            F.col("state_code").alias("State"),
        )
        .dropDuplicates()
    )
def extract_oracle_private_clients(
    df: DataFrame,
    holders: DataFrame
) -> DataFrame:

    holder_keys = (
        holders
        .select("holder_context")
        .distinct()
    )

    return (
        df
        .filter(F.col("holder_type") == "PERSON")
        .join(
            F.broadcast(holder_keys),
            on="holder_context",
            how="left_semi"
        )
        .select(
            "holder_context",
            "AB_PRENOM",
            "AB_NOM",
            "AB_ADDRESS",
            "AB_EMAIL",
            "sex",
            F.col("birth_year").cast("int").alias("birth_year"),
            "state_code"
        )
        .dropDuplicates()
    )