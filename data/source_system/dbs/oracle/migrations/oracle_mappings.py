import re

from data.source_system.bootstrap.column_mapping import STRUCT_MAPPINGS


ORACLE_TABLES = {
    "property_location_context": "POLICY_PROPERTY_LOCATION",
    "holder_classification_context": "POLICY_HOLDER_CLASSIFICATION",
    "property_usage": "POLICY_PROPERTY_USAGE",
    "building_technical_properties": "POLICY_BUILDING_TECHNICAL",
    "building_risk_context": "POLICY_BUILDING_RISK",
    "policy_contract_context": "POLICY_CONTRACT",
    "policy_rating_context": "POLICY_RATING",
    "premium_financial_context": "POLICY_PREMIUM_FINANCIAL",
}


def to_snake_case(name: str) -> str:
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower()


ORACLE_STRUCT_MAPPINGS = {
    struct_name: {
        "target": f"ASSUREPLUS.{ORACLE_TABLES[struct_name]}",
        "columns": {
            source: to_snake_case(target)
            for source, target in config["columns"].items()
        },
    }
    for struct_name, config in STRUCT_MAPPINGS.items()
}

CLAIM_COLUMNS = [
    "claim_id",
    "policy_record_id",
    "as_of_date",
    "date_of_loss",
    "year_of_loss",
    "house_worship",
    "number_of_floors_in_the_insured_building",
    "amount_paid_on_building_claim",
    "amount_paid_on_contents_claim",
    "amount_paid_on_increased_cost_of_compliance_claim",
    "building_damage_amount",
    "net_building_payment_amount",
    "building_property_value",
    "cause_of_damage",
    "contents_damage_amount",
    "net_contents_payment_amount",
    "contents_property_value",
    "disaster_assistance_coverage_required",
    "event_designation_number",
    "fico_number",
    "flood_characteristics_indicator",
    "flood_water_duration",
    "flood_event",
    "icc_coverage",
    "net_icc_payment_amount",
    "non_payment_reason_contents",
    "non_payment_reason_building",
    "number_of_units",
    "contents_replacement_cost",
    "replacement_cost_basis",
    "water_depth",
    "county_code",
    "open_date",
    "most_recent_recovery_date",
    "exterior_water_depth",
    "interior_water_depth",
    "most_recent_payment_date",
    "pre_firm_indicator",
    "total_salvage_recovery",
    "total_bldg_claim_pmt_recovery",
    "total_contents_claim_pmt_recovery",
    "total_icc_claim_pmt_recovery",
    "total_subrogation_recovery",
]