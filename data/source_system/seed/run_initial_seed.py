
from pathlib import Path

from data.source_system.dbs.oracle.migrations.oracle_mappings import ORACLE_STRUCT_MAPPINGS, to_snake_case, \
    CLAIM_COLUMNS
from data.source_system.seed.extractors import extract_oracle_holders, extract_oracle_private_clients, extract_struct
from pyspark.sql import functions as F

from data.spark_utils.data_utils import create_spark_session
from data.source_system.seed.jdbc_loader import read_jdbc, write_jdbc

ROOT = Path(__file__).resolve().parents[3]


def main():
    spark = create_spark_session("AsssurPlus Seed")

    try:
        # oracle_policies = spark.read.parquet(
        #     str(ROOT / "data/results/policies_with_holders_v1")
        # )
        oracle_claims = spark.read.parquet(
            str(ROOT / "data/results/oracle_claims_with_p_id_server_v1")
        )
        oracle_claims.show()
        oracle_claims.printSchema()
        policies = read_jdbc(
            spark=spark,
            db_url="ORACLE_JDBC_URL",
            user="ORACLE_USER",
            password="ORACLE_PASSWORD",
            table="ASSUREPLUS.policy",
            driver="oracle.jdbc.OracleDriver",
        )

        policy_keys = (
            policies
            .select(F.col("POLICY_RECORD_ID").alias("policy_record_id"))
            .distinct()
        )

        claims = oracle_claims.join(policy_keys, on="policy_record_id", how="left_semi")
        print("Claims source:", oracle_claims.count())
        print("Policies Oracle:", policy_keys.count())
        print("Claims matching policies:", claims.count())
        claims_clean = claims.select(
            *[
                F.col(c).alias(to_snake_case(c))
                for c in claims.columns
            ]
        )
        oracle_claims.select("policy_record_id").show(10)
        policy_keys.show(10)
        claims_clean.show(10)
        claims_clean.printSchema()
        claims_ready = claims_clean.select(*CLAIM_COLUMNS)
        claims_ready.show()
        claims_ready.printSchema()
        # for struct_name, config in ORACLE_STRUCT_MAPPINGS.items():
        #     extracted = extract_struct(
        #         df=oracle_policies,
        #         struct_name=struct_name,
        #         mapping=config["columns"],
        #         holder_keys=holder_keys,
        #     )
        #
        #     extracted = extracted.join(
        #         policy_keys,
        #         on="policy_record_id",
        #         how="left_semi",
        #     )
        #
        write_jdbc(
        df=claims_ready,
        db_url="ORACLE_JDBC_URL",
        user="ORACLE_USER",
        password="ORACLE_PASSWORD",
        table="ASSUREPLUS.CLAIM",
        driver="oracle.jdbc.OracleDriver",
        partitions=2,
        batch_size=10_000,
    )
        #
        #     print(f"Loaded: {config['target']}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
