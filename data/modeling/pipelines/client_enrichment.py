from dataclasses import dataclass, field
from collections.abc import Callable
from data.spark_utils.data_utils import create_spark_session
from data.ingestion.syntetic_data_adaptation.create_ab_client import (
    create_ab_client,
    create_holders,
    enrich_holders_filtered_single_with_sex_and_birthdate,
    prepare_clients_with_surname_ticket,
    join_clients_with_ssa,
    enrich_client_with_ssa_names,
    fill_null_names_with_john,
    enrich_clients_with_addresses,
    join_policies_with_holders,
)

@dataclass
class ClientEnrichmentPipeline:
    db_name: str
    steps: list[Callable[[], object]] = field(default_factory=list)
    def __call__(self,start_at: int = 1):
        for index, step in enumerate(self.steps, start=1):
            if start_at > index:
                continue
            print(f"[{self.db_name}] Step {index}/{len(self.steps)}")
            try:
                step()
            except Exception:
                print(f"[{self.db_name}] FAILED at step {index}")
                raise

            print(f"[{self.db_name}] Completed step {index}")

def build_pipeline(spark, db_name):

    def read_checkpoint(name):
        return spark.read.parquet(
            f"data/results/{db_name}_{name}"
        )

    return ClientEnrichmentPipeline(
        db_name=db_name,
        steps=[
            lambda: create_ab_client(spark, db_name),
            lambda: create_holders(
                read_checkpoint("customer_base"),
                db_name
            ),
            lambda: enrich_holders_filtered_single_with_sex_and_birthdate(
                spark, db_name
            ),
            lambda: prepare_clients_with_surname_ticket(
                spark,
                db_name
            ),
            lambda: join_clients_with_ssa(spark, db_name),
            lambda: enrich_client_with_ssa_names(spark, db_name),
            lambda: fill_null_names_with_john(spark, db_name),
            lambda: enrich_clients_with_addresses(spark, db_name),
            lambda: join_policies_with_holders(spark, db_name),
        ]
    )

if __name__ == "__main__":
    spark = create_spark_session("Client Enrichment Oracle")

    try:
        pipeline = build_pipeline(spark, "oracle")
        pipeline(start_at=6)
    finally:
        spark.stop()