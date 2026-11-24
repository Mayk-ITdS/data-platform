"""FEMA canonical Silver extensions from Debezium Iceberg Bronze.
Reads lakehouse.silver.source_schema_inventory and reconstructs latest CDC state
per source primary key. Writes canonical union tables (both systems) for ten
business entities, plus SQL Server technical policy_unit relationship table.
Does NOT replace existing lakehouse.silver.policy and lakehouse.silver.claim.
Batch reconstruction, not an incremental MERGE job. Source record JSON is kept.
"""

from __future__ import annotations
import base64
import re
from decimal import Decimal
from pyspark.sql import SparkSession, Window, functions as F, types as T

SOURCE_FIELD_MAPPINGS = {
    "ORACLE": {
        "PRIVATE_CLIENT": {
            "ab_prenom": "first_name",
            "ap_prenom": "first_name",
            "ab_nom": "last_name",
            "ap_nom": "last_name",
            "ab_address": "address",
            "ap_address": "address",
            "ab_email": "email",
            "ap_email": "email",
        }
    }
}
PREFIX = {
    "SQLSERVER": "abassurance.assurances.dbo.",
    "ORACLE": "assureplus_v2.ASSUREPLUS.",
}
ALIASES = {
    "Building": {"number_of_floors_in_insured_building": "insured_building_floors"},
    "PolicyTerms": {
        "total_building_insurance_coverage": "total_building_coverage",
        "total_contents_insurance_coverage": "total_contents_coverage",
        "cancellation_date_of_flood_policy": "flood_policy_cancel_date",
        "cancellation_voidance_reason_code": "cancel_void_reason_code",
    },
    "PremiumFinancial": {
        "total_insurance_premium_of_the_policy": "total_policy_insurance_premium",
    },
    "Risks": {
        "basement_enclosure_crawlspace_type": "basement_crawlspace_type",
        "disaster_assistance_coverage_required_code": "disaster_assistance_req_code",
        "elevation_certificate_indicator": "elevation_certificate_flag",
        "post_firm_construction_indicator": "post_firm_construction_flag",
        "regular_emergency_program_indicator": "regular_emergency_program",
    },
    "HolderClassification": {
        "small_business_indicator_building": "small_business_building_flag",
    },
    "PropertyUsage": {},
    "Claims": {},
}

EXTENSIONS = [
    ("policy_terms", "PolicyTerms", "POLICY_CONTRACT", "PolicyTermsId"),
    ("building", "Building", "POLICY_BUILDING_TECHNICAL", "BuildingId"),
    ("risks", "Risks", "POLICY_BUILDING_RISK", "RiskId"),
    ("property", "Property", "POLICY_PROPERTY_LOCATION", "PropertyId"),
    ("property_usage", "PropertyUsage", "POLICY_PROPERTY_USAGE", "PropertyUsageId"),
    ("policy_rating", "PolicyRating", "POLICY_RATING", "PolicyRatingId"),
    ("premium_financial", "PremiumFinancial", "POLICY_PREMIUM_FINANCIAL", "PremiumFinancialId"),
    ("holder_classification", "HolderClassification", "POLICY_HOLDER_CLASSIFICATION", "HolderClassificationId"),
]

@F.udf(T.StringType())
def decode_oracle_integer(value):
    if value is None:
        return None
    try:
        return str(int.from_bytes(base64.b64decode(value), "big", signed=True))
    except Exception as exc:
        raise ValueError(f"Invalid Oracle integer: {value!r}") from exc


def decimal_udf(precision, scale):
    @F.udf(T.DecimalType(precision, scale))
    def decode(value):
        if value is None:
            return None
        raw = base64.b64decode(value)
        return Decimal(int.from_bytes(raw, "big", signed=True)).scaleb(-scale)

    return decode

def jsonfield(json_col, name):
    return F.get_json_object(F.col(json_col), f"$.{name}")

def latest_raw(system, table, pk):
    topic = PREFIX[system] + table
    if topic not in META:
        raise ValueError(f"Topic absent from schema inventory: {topic}")
    src = (spark.table("lakehouse.bronze.cdc_events")
           .where(F.col("topic") == topic)
           .where(F.col("value_raw").isNotNull())
           .select("source_system", "topic", "partition", "offset", "kafka_timestamp",
                   F.col("key_raw").cast("string").alias("key_json"),
                   F.col("value_raw").cast("string").alias("envelope"))
           .withColumn("op", jsonfield("envelope", "payload.op"))
           .where(F.col("op").isin("r", "c", "u", "d"))
           .withColumn("after_json", jsonfield("envelope", "payload.after"))
           .withColumn("before_json", jsonfield("envelope", "payload.before"))
           .withColumn("record_json", F.coalesce("after_json", "before_json"))
           .withColumn("_pk", F.coalesce(jsonfield("after_json", pk),
                                         jsonfield("before_json", pk),
                                         jsonfield("key_json", pk))))
    if system == "ORACLE" and pk in ("POLICY_RECORD_ID", "CLAIM_ID"):
        src = src.withColumn("_pk", decode_oracle_integer("_pk"))

    w = Window.partitionBy("_pk").orderBy(
        F.col("offset").desc()
    )

    return (
        src.where(F.col("_pk").isNotNull())
        .withColumn("_rank", F.row_number().over(w))
        .where(F.col("_rank") == 1)
        .where(F.col("op") != "d")
        .drop("_rank")
    )

def typed_value(row, meta):
    raw = jsonfield("record_json", meta["source_column"])
    logical = meta["logical_type"]
    connect = meta["connect_type"]
    if logical == "org.apache.kafka.connect.data.Decimal":
        scale = int(meta["decimal_scale"])
        precision = int(meta["decimal_precision"])
        return decimal_udf(precision, scale)(raw)
    if logical == "io.debezium.time.Date":
        return F.date_add(F.lit("1970-01-01").cast("date"), raw.cast("int"))
    if logical in ("io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp"):
        divisor = 1_000_000 if "Micro" in logical else 1000
        return F.timestamp_micros((raw.cast("long") * (1_000_000 // divisor)))
    if connect == "boolean":
        return raw.cast("boolean")
    if connect in ("int8", "int16", "int32", "int64"):
        return raw.cast("long")
    if connect in ("float32", "float64"):
        return raw.cast("double")
    return raw.cast("string")

def normalize_source_column(name: str, system: str) -> str:
    if system == "ORACLE" and name.startswith("ab_"):
        return "ap_" + name[3:]
    return name

def extract(system, table, pk, alias_group=None):
    df = latest_raw(system, table, pk)
    topic = PREFIX[system] + table
    aliases = ALIASES.get(alias_group or table, {})
    cols = []
    names = set()
    mapping = SOURCE_FIELD_MAPPINGS.get(system, {}).get(table, {})
    for m in META[topic]:
        original = m["normalized_column"]
        name = mapping.get(
            original,
            aliases.get(original, original)
        )
        if name in names:
            raise ValueError(
                f"Duplicate canonical column for {topic}: {name}"
            )
        names.add(name)
        value = typed_value(df, m)

        if system == "ORACLE" and table == "PRIVATE_CLIENT":
            field = original.lower()

            if field in {
                "ab_prenom", "ap_prenom",
                "ab_nom", "ap_nom",
                "ab_address", "ap_address",
                "ab_email", "ap_email",
            }:
                suffix = field.split("_", 1)[1].upper()

                value = F.coalesce(
                    F.get_json_object(
                        F.col("record_json"),
                        f"$.AP_{suffix}"
                    ),
                    F.get_json_object(
                        F.col("record_json"),
                        f"$.AB_{suffix}"
                    )
                )

        cols.append(value.alias(name))

    return df.select(
        F.lit(system).alias("source_system"),
        F.col("_pk").alias("source_primary_key"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("kafka_timestamp").alias("source_kafka_timestamp"),
        F.col("op").alias("last_cdc_operation"),
        F.col("record_json").alias("source_record_json"),
        *cols,
    )

def harmonize(left, right):
    """Unify independently generated SQL/Oracle types before union."""
    from pyspark.sql.types import (
        BooleanType, DateType, TimestampType, IntegralType,
        NumericType, StringType, DecimalType,
    )
    left_fields = {f.name: f.dataType for f in left.schema.fields}
    right_fields = {f.name: f.dataType for f in right.schema.fields}
    for name in sorted(set(left_fields) & set(right_fields)):
        a, b = left_fields[name], right_fields[name]
        if a == b:
            continue
        if (isinstance(a, BooleanType) and isinstance(b, IntegralType)):
            right = right.withColumn(name, (F.col(name) != 0).cast("boolean"))
        elif (isinstance(b, BooleanType) and isinstance(a, IntegralType)):
            left = left.withColumn(name, (F.col(name) != 0).cast("boolean"))
        elif ((isinstance(a, DateType) and isinstance(b, TimestampType)) or
              (isinstance(b, DateType) and isinstance(a, TimestampType))):
            left = left.withColumn(name, F.col(name).cast("timestamp"))
            right = right.withColumn(name, F.col(name).cast("timestamp"))
        elif isinstance(a, DecimalType) and isinstance(b, DecimalType):
            precision = max(a.precision, b.precision)
            scale = max(a.scale, b.scale)
            if precision > 38:
                raise ValueError(f"Decimal precision > 38: {name}")
            target = T.DecimalType(precision, scale)
            left = left.withColumn(name, F.col(name).cast(target))
            right = right.withColumn(name, F.col(name).cast(target))
        else:
            raise ValueError(f"Unresolved canonical type mismatch {name}: {a} vs {b}")
    return left, right


def sql_pk_to_snake(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

def save(df, name):
    full_name = f"lakehouse.silver.{name}"
    (df.writeTo(full_name).using("iceberg").createOrReplace())
    print("Written:", full_name, flush=True)

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("FEMA-Canonical-Silver-Extensions")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.lakehouse.type", "hadoop")
             .config("spark.sql.catalog.lakehouse.warehouse", "/lakehouse/warehouse")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    INV = spark.table("lakehouse.silver.source_schema_inventory").collect()
    META = {}
    for row in INV:
        META.setdefault(row["topic"], []).append(row.asDict())

    unit = extract("SQLSERVER", "PolicyUnit", "PolicyUnitId")
    unit = unit.withColumn("canonical_policy_key",
        F.concat_ws(":", F.lit("SQLSERVER"), F.col("policy_id").cast("string")))
    save(unit, "policy_unit")

    for canonical, sql_table, oracle_table, fk in EXTENSIONS:
        sql_pk = {
            "Risks": "RiskId", "Building": "BuildingId", "PolicyTerms": "PolicyTermsId",
            "Property": "PropertyId", "PropertyUsage": "PropertyUsageId",
            "PolicyRating": "PolicyRatingId", "PremiumFinancial": "PremiumFinancialId",
            "HolderClassification": "HolderClassificationId",
        }[sql_table]
        sql = extract("SQLSERVER", sql_table, sql_pk)
        fk_normalized = re.sub(r"(?<!^)(?=[A-Z])", "_", fk).lower()
        links = (unit.select(
                F.col(fk_normalized).alias("_fk"),
                F.col("policy_id").cast("string").alias("policy_record_id"),
                F.col("policy_unit_id"))
            .where(F.col("_fk").isNotNull()))
        sql = (sql.join(links, sql[sql_pk_to_snake(sql_pk)] == links["_fk"], "left")
               .drop("_fk"))
        ora = extract("ORACLE", oracle_table, "POLICY_RECORD_ID", sql_table)
        ora = ora.withColumn("policy_record_id", F.col("policy_record_id").cast("string"))
        if "policy_unit_id" not in ora.columns:
            ora = ora.withColumn("policy_unit_id", F.lit(None).cast("long"))
        sql, ora = harmonize(sql, ora)
        merged = sql.unionByName(ora, allowMissingColumns=True)
        merged = merged.withColumn("canonical_policy_key",
            F.when(F.col("policy_record_id").isNotNull(),
                   F.concat_ws(":", F.col("source_system"), F.col("policy_record_id"))))
        save(merged, canonical)

    sql_holder = extract("SQLSERVER", "Holder", "HolderId")
    ora_holder = extract("ORACLE", "HOLDER", "HOLDER_CONTEXT")
    sql_holder, ora_holder = harmonize(sql_holder, ora_holder)
    holders = sql_holder.unionByName(ora_holder, allowMissingColumns=True)
    holders = holders.withColumn("canonical_holder_key",
        F.concat_ws(":", F.col("source_system"), F.col("holder_context")))
    save(holders, "holder")

    sql_private = extract("SQLSERVER", "PrivateClient", "HolderId")
    lookup = sql_holder.select(F.col("holder_id").alias("_holder_fk"), "holder_context")
    sql_private = (sql_private.join(lookup, sql_private["holder_id"] == lookup["_holder_fk"], "left")
                   .drop("_holder_fk"))
    ora_private = extract("ORACLE", "PRIVATE_CLIENT", "HOLDER_CONTEXT")
    sql_private, ora_private = harmonize(sql_private, ora_private)
    clients = sql_private.unionByName(ora_private, allowMissingColumns=True)
    clients = clients.withColumn("canonical_holder_key",
        F.concat_ws(":", F.col("source_system"), F.col("holder_context")))
    save(clients, "private_client")

    spark.stop()
