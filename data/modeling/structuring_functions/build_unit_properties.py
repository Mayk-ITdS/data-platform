from pyspark.sql import functions as F

from data.modeling.structuring_functions.struct_models import build_premium_financial_context_struct, \
    build_property_location_context_struct, build_holder_classification_context_struct, build_property_usage_struct, \
    build_building_technical_properties_struct, build_building_risk_context_struct, \
    build_policy_contract_context_struct, build_policy_rating_context_struct

def build_unit_properties_struct():
    return F.struct(
        F.col('policy_record_id'),
        build_property_location_context_struct(),
        build_holder_classification_context_struct(),
        build_property_usage_struct(),
        build_building_technical_properties_struct(),
        build_building_risk_context_struct(),
        build_policy_contract_context_struct(),
        build_policy_rating_context_struct(),
        build_premium_financial_context_struct(),

    ).alias('unit_properties')