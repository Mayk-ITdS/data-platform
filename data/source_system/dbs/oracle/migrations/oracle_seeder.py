from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, ClassVar
from functools import wraps
from time import perf_counter

from data.source_system.dbs.oracle.migrations.oracle_mappings import ORACLE_STRUCT_MAPPINGS
from data.spark_utils.data_utils import create_spark_session
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from data.source_system.seed.extractors import (
    extract_oracle_holders,
    extract_oracle_private_clients, extract_struct, extract_oracle_policies,
)
from data.source_system.seed.jdbc_loader import write_jdbc, read_jdbc


def log_step(func):

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        start = perf_counter()

        print(f"[ORACLE] START {func.__name__}")

        try:
            result = func(self, *args, **kwargs)

        except Exception:
            print(f"[ORACLE] FAILED {func.__name__}")
            raise

        finally:
            elapsed = perf_counter() - start
            print(f"[ORACLE] Duration: {elapsed:.2f}s")

        print(f"[ORACLE] COMPLETED {func.__name__}")

        return result

    return wrapper

@dataclass
class OracleSeeder:
    spark: SparkSession
    df_path: Path
    holder_limit: int = 10_000
    struct_names: ClassVar[dict] =ORACLE_STRUCT_MAPPINGS
    steps: list[Callable[[], None]] = field(
        init=False,
        repr=False,
    )
    source: DataFrame = field(
        init=False,
        repr=False,
    )
    holders: DataFrame = field(
        init=False,
        repr=False,
    )
    policy_source: DataFrame = field(
        init=False,
        repr=False,
    )
    policy_keys:DataFrame = field(
        init=False,
        repr=False,
    )

    def __post_init__(self):
        self.steps = [
            self.load_source,
            self.validate_source,
            self.seed_holders,
            self.seed_private_clients,
            self.seed_policies,
            self.seed_structs,
        ]

    def run_from(self, step: int):
        self.load_source()
        self.validate_source()

        if step <= 3:
            self.seed_holders()
        else:
            self.load_existing_holders()

        if step <= 4:
            self.seed_private_clients()

        if step <= 5:
            self.seed_policies()

        if step <= 6:
            self.seed_structs()
    @log_step
    def load_source(self):
        self.source = self.spark.read.parquet(
            str(self.df_path)
        )
    @log_step
    def load_existing_holders(self):
        self.holders = read_jdbc(
            spark=self.spark,
            db_url="ORACLE_JDBC_URL",
            user="ORACLE_USER",
            password="ORACLE_PASSWORD",
            driver="oracle.jdbc.OracleDriver",
            table="ASSUREPLUS.HOLDER"
        ).select("holder_context")
    @log_step
    def validate_source(self):
        invalid = self.source.filter(
            F.col("source_system").isNull()
            | (F.col("source_system") != "ORACLE")
        )

        if invalid.limit(1).count():
            raise ValueError(
                "Expected source_system=ORACLE"
            )

    def write(self, df: DataFrame, table: str):
        if "." not in table:
            table = f"ASSUREPLUS.{table}"
        write_jdbc(
            df=df,
            db_url="ORACLE_JDBC_URL",
            user="ORACLE_USER",
            password="ORACLE_PASSWORD",
            table=table,
            driver="oracle.jdbc.OracleDriver",
            partitions=2,
        )

    @log_step
    def seed_holders(self):
        self.holders = (
            extract_oracle_holders(self.source)
            .orderBy("holder_context")
            .limit(self.holder_limit)

        )

        self.holders.count()

        self.write(
            self.holders,
            "HOLDER",
        )

    @log_step
    def seed_private_clients(self):
        clients = extract_oracle_private_clients(
            self.source,
            self.holders,
        )

        self.write(
            clients,
            "PRIVATE_CLIENT",
        )

    @log_step
    def seed_policies(self):

        holder_keys = self.holders.select(
            "holder_context"
        ).distinct()

        self.policy_source = (
            self.source
            .join(
                holder_keys,
                on="holder_context",
                how="left_semi",
            )

        )

        self.policy_source.count()

        policies = extract_oracle_policies(self.policy_source)
        self.policy_keys = policies.select("policy_record_id").distinct()

        self.write(policies, "POLICY")

    @log_step
    def seed_structs(self):

        for struct_name, config in self.struct_names.items():
            struct_df = extract_struct(
                df=self.policy_source,
                struct_name=struct_name,
                mapping=config['columns'],
                policy_keys=self.policy_keys,
            )

            table_name = config['target']

            self.write(struct_df, table_name)
    def __call__(self):
        try:
            for index, step in enumerate(self.steps, start=1):
                print(
                    f"[ORACLE] Step {index}/{len(self.steps)}"
                )
                step()
        finally:
            if hasattr(self, "policy_keys"):
                self.policy_keys.unpersist()

            if hasattr(self, "policy_source"):
                self.policy_source.unpersist()

            if hasattr(self, "holders"):
                self.holders.unpersist()

def main():
    spark = create_spark_session("Seed Oracle")

    try:
        seeder = OracleSeeder(
            spark=spark,
            df_path=(
                Path(__file__).resolve().parents[5]
                / "data/results/oracle_policies_with_holders_v1"
            ),
        )

        seeder.run_from(5)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()