from data.data_dict import key_columns

def allocate_policies(policies,spark_session):
    splitting_cols = ['reportedZipCode', 'censusGeoid']

    policies_with_signatures = (
                                 policies
                                .withColumn('group_signature',
                                                   stable_hash(key_columns)
                                            )
                                .withColumn('splitting_hash',
                                            stable_hash(splitting_cols)))

    splitting_hash_analysis = (
        policies_with_signatures
        .groupBy(
            "splitting_hash",
            "reportedZipCode",
            "censusGeoid"
        )
        .agg(
            F.count("*").alias("rows_count")
        )
    ).persist(StorageLevel.MEMORY_AND_DISK)

    total_rows = (
        splitting_hash_analysis
        .agg(F.sum("rows_count"))
        .first()[0]
    )

    target_70 = total_rows * 0.70

    cumulative_window = Window.orderBy('splitting_hash').rowsBetween(Window.unboundedPreceding, Window.currentRow)

    split_set_final = (
        splitting_hash_analysis
        .withColumn(
            'accumulative_rows_sum',
            F.sum('rows_count').over(cumulative_window)
        )
    ).withColumn('source_system',
                  F.when(
                      F.col(
                          'accumulative_rows_sum'
                      ) <= target_70,
                      F.lit('SQL_SERVER')
                  ).otherwise(F.lit('ORACLE')))

    p = policies_with_signatures.alias('p')
    sf = split_set_final.alias('sf')

    joined = p.join(sf, on='splitting_hash', how='inner').select('p.*',sf.accumulative_rows_sum, sf.rows_count,sf.source_system)

    return joined
