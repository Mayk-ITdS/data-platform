from typing import Dict, Callable

from data.source_system.dbs.oracle.migrations.oracle_mappings import CLAIM_COLUMNS, to_snake_case
from data.source_system.seed.jdbc_loader import write_jdbc, read_jdbc
from data.spark_utils.data_utils import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def extract_holders(df: DataFrame) -> DataFrame:
    return (
        df.filter((F.col("holder_type").isNotNull())&
                  F.col("AB_ADDRESS")
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
def extract_oracle_policies(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("unit_properties.policy_record_id").alias("policy_record_id"),
            F.col("holder_context"),
            F.lit("ORACLE").alias("source_system"),
            F.col("splitting_hash").alias("splitting_hash"),
            F.col("group_signature").alias("group_signature"),
            F.col("series_signature").alias("series_signature"),
            F.col("geo_group_series").alias("geo_group_series"),
            F.col("originalNBDate").alias("original_nb_date"),
        ).dropDuplicates(["policy_record_id"])
    )
def extract_struct(
    df: DataFrame,
    struct_name: str,
    mapping:Dict,
    policy_keys: DataFrame
) -> DataFrame:

    extracted = df.select(
        F.col('unit_properties.policy_record_id').alias('policy_record_id'),
        *[
            F.col(f'unit_properties.{struct_name}.{source}').alias(target)
            for source, target in mapping.items()
        ]
    )
    return (extracted.join(
        policy_keys,
        on='policy_record_id',
        how='left_semi'
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

    )
def extract_claims_oracle(spark) -> DataFrame:
    policy_keys = read_jdbc(
        spark=spark,
        db_url="ORACLE_JDBC_URL",
        user="ORACLE_USER",
        password="ORACLE_PASSWORD",
        driver="oracle.jdbc.driver.OracleDriver",
        table="ASSUREPLUS.POLICY",
    ).select(F.col('POLICY_RECORD_ID').alias('policy_record_id')).distinct()

    claims_oracle = spark.read.parquet("data/results/oracle_claims_with_p_id_server_v1")
    claims_cols_trans = [to_snake_case(col) for col in claims_oracle.columns]
    claims_oracle = claims_oracle.toDF(*claims_cols_trans)
    claims_oracle = claims_oracle.join(policy_keys,on='policy_record_id',how='left_semi').select(*CLAIM_COLUMNS).dropDuplicates(['policy_record_id'])
    claims_oracle.printSchema()
    write_jdbc(
        df=claims_oracle,
        db_url="ORACLE_JDBC_URL",
        user="ORACLE_USER",
        password="ORACLE_PASSWORD",
        table="ASSUREPLUS.CLAIM",
        driver="oracle.jdbc.driver.OracleDriver",

    )
    return claims_oracle


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
            holder_keys,
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
if __name__=="__main__":
    spark = create_spark_session("Extract Oracle Claims")
    extract_claims_oracle(spark)