import logging
from data.spark_utils.data_utils import create_spark_session

def get_dbs():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(__name__)

    spark = create_spark_session('Retrieving databases')
    edges_output_path = "data/results/claim_policy_edges"

    claims_full = (
        spark.read.option('recursiveFileLookup', True)
        .parquet('data/canonical/claims')
        .withColumnsRenamed({"id": 'claim_record_id','numberOfFloorsInTheInsuredBuilding':'numberOfFloorsInInsuredBuilding'})
    )
    policies_full = (
        spark.read.option('recursiveFileLookup',True)
        .parquet('data/canonical/policies')
        .withColumnsRenamed({"id":'policy_record_id',"preFIRMConstructionIndicator":'preFIRMIndicator'}).drop('asOfDate')
                     )

    computed_edges = spark.read.parquet(
        edges_output_path
    )
    logger.info('Ended Compute -- Returning Databases')

    return  policies_full,claims_full, computed_edges,spark