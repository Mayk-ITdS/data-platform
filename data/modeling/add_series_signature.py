from data.spark_utils.data_utils import stable_hash

def add_series_signature(policies,signature,columns):

    return policies.withColumn(
        signature,
        stable_hash(columns)
    )