WHENEVER SQLERROR EXIT SQL.SQLCODE;
SET ECHO ON;
SET SERVEROUTPUT ON;
SHOW USER;
SHOW CON_NAME;

CREATE TABLE holder (
    holder_context VARCHAR2(64 CHAR) NOT NULL,
    holder_type VARCHAR2(30 CHAR) NOT NULL,
    CONSTRAINT pk_holder PRIMARY KEY (holder_context),
    CONSTRAINT ck_holder_type CHECK (holder_type IN ('PERSON','COMMERCIAL_ORGANIZATION','MIXED_HOLDER'))
);

CREATE TABLE private_client (
    holder_context VARCHAR2(64 CHAR) NOT NULL,
    ab_prenom VARCHAR2(150 CHAR) NOT NULL,
    ab_nom VARCHAR2(150 CHAR) NOT NULL,
    ab_address VARCHAR2(500 CHAR) NOT NULL,
    ab_email VARCHAR2(320 CHAR),
    sex CHAR(1 CHAR),
    birth_year NUMBER(4),
    state_code VARCHAR2(12 CHAR),
    CONSTRAINT pk_private_client PRIMARY KEY (holder_context),
    CONSTRAINT fk_pc_holder FOREIGN KEY (holder_context) REFERENCES holder(holder_context),
    CONSTRAINT ck_pc_sex CHECK (sex IN ('F','M')),
    CONSTRAINT ck_pc_year CHECK (birth_year BETWEEN 1850 AND 2100)
);

CREATE TABLE policy (
    policy_record_id NUMBER(19) NOT NULL,
    holder_context VARCHAR2(64 CHAR) NOT NULL,
    source_system VARCHAR2(20 CHAR) DEFAULT 'ORACLE' NOT NULL,
    splitting_hash VARCHAR2(64 CHAR),
    group_signature VARCHAR2(64 CHAR),
    series_signature VARCHAR2(64 CHAR),
    geo_group_series VARCHAR2(64 CHAR),
    original_nb_date DATE,
    CONSTRAINT pk_policy PRIMARY KEY (policy_record_id),
    CONSTRAINT fk_policy_holder FOREIGN KEY (holder_context) REFERENCES holder(holder_context)
);

CREATE TABLE claim (
    claim_id NUMBER(19) NOT NULL,
    policy_record_id NUMBER(19) NOT NULL,
    as_of_date TIMESTAMP(0),
    date_of_loss DATE,
    year_of_loss NUMBER(4),
    house_worship NUMBER(1),
    number_of_floors_in_the_insured_building NUMBER(5),
    amount_paid_on_building_claim NUMBER(20,4),
    amount_paid_on_contents_claim NUMBER(20,4),
    amount_paid_on_increased_cost_of_compliance_claim NUMBER(20,4),
    building_damage_amount NUMBER(20,4),
    net_building_payment_amount NUMBER(20,4),
    building_property_value NUMBER(20,4),
    cause_of_damage NUMBER(5),
    contents_damage_amount NUMBER(20,4),
    net_contents_payment_amount NUMBER(20,4),
    contents_property_value NUMBER(20,4),
    disaster_assistance_coverage_required NUMBER(5),
    event_designation_number VARCHAR2(40 CHAR),
    fico_number NUMBER(10),
    flood_characteristics_indicator VARCHAR2(30 CHAR),
    flood_water_duration NUMBER(10),
    flood_event VARCHAR2(255 CHAR),
    icc_coverage NUMBER(20,4),
    net_icc_payment_amount NUMBER(20,4),
    non_payment_reason_contents VARCHAR2(15 CHAR),
    non_payment_reason_building VARCHAR2(15 CHAR),
    number_of_units NUMBER(10),
    contents_replacement_cost NUMBER(20,4),
    replacement_cost_basis VARCHAR2(15 CHAR),
    water_depth NUMBER(10),
    county_code VARCHAR2(12 CHAR),
    open_date DATE,
    most_recent_recovery_date DATE,
    exterior_water_depth NUMBER(10),
    interior_water_depth NUMBER(10),
    most_recent_payment_date DATE,
    pre_firm_indicator NUMBER(1),
    total_salvage_recovery NUMBER(20,4),
    total_bldg_claim_pmt_recovery NUMBER(20,4),
    total_contents_claim_pmt_recovery NUMBER(20,4),
    total_icc_claim_pmt_recovery NUMBER(20,4),
    total_subrogation_recovery NUMBER(20,4),
    CONSTRAINT pk_claim PRIMARY KEY (claim_id),
    CONSTRAINT uq_claim_policy UNIQUE (policy_record_id),
    CONSTRAINT fk_claim_policy FOREIGN KEY (policy_record_id) REFERENCES policy(policy_record_id),
    CONSTRAINT ck_claim_house CHECK (house_worship IN (0,1)),
    CONSTRAINT ck_claim_pre_firm CHECK (pre_firm_indicator IN (0,1))
);




CREATE TABLE policy_property_location (
    policy_record_id NUMBER(19) NOT NULL,
    reported_city VARCHAR2(150 CHAR),
    reported_zip_code VARCHAR2(12 CHAR),
    census_geoid VARCHAR2(20 CHAR),
    latitude NUMBER(9,1),
    longitude NUMBER(9,1),
    fema_region NUMBER(3),
    nfip_community_name VARCHAR2(255 CHAR),
    nfip_community_number_current VARCHAR2(30 CHAR),
    nfip_rated_community_number VARCHAR2(30 CHAR),
    map_panel_number VARCHAR2(30 CHAR),
    map_panel_suffix VARCHAR2(10 CHAR),
    CONSTRAINT pk_pol_location PRIMARY KEY (policy_record_id),
    CONSTRAINT fk_pol_location FOREIGN KEY (policy_record_id) REFERENCES policy(policy_record_id)
);

CREATE TABLE policy_holder_classification (
    policy_record_id NUMBER(19) NOT NULL,
    state_owned_indicator NUMBER(1),
    non_profit_indicator NUMBER(1),
    house_of_worship_indicator NUMBER(1),
    small_business_indicator_building NUMBER(1),
    condominium_coverage_type_code VARCHAR2(10 CHAR),
    condo_coverage_category VARCHAR2(80 CHAR),
    coverage_signal VARCHAR2(80 CHAR),
    CONSTRAINT pk_pol_holder_class PRIMARY KEY (policy_record_id),
    CONSTRAINT fk_pol_holder_class FOREIGN KEY (policy_record_id) REFERENCES policy(policy_record_id)
);

CREATE TABLE policy_property_usage (
    policy_record_id NUMBER(19) NOT NULL,
    occupancy_type NUMBER(5),
    occupancy_category VARCHAR2(100 CHAR),
    primary_residence_indicator NUMBER(1),
    rental_property_indicator NUMBER(1),
    tenant_indicator NUMBER(1),
    seasonally_occupied NUMBER(1),
    building_purpose VARCHAR2(30 CHAR),
    CONSTRAINT pk_pol_usage PRIMARY KEY (policy_record_id),
    CONSTRAINT fk_pol_usage FOREIGN KEY (policy_record_id) REFERENCES policy(policy_record_id)
);

CREATE TABLE policy_building_technical (
    policy_record_id NUMBER(19) NOT NULL,
    original_construction_date DATE,
    building_description_code NUMBER(5),
    building_category VARCHAR2(100 CHAR),
    number_of_floors_in_insured_building NUMBER(5),
    construction NUMBER(1),
    CONSTRAINT pk_pol_build_tech PRIMARY KEY (policy_record_id),
    CONSTRAINT fk_pol_build_tech FOREIGN KEY (policy_record_id) REFERENCES policy(policy_record_id)
);

CREATE TABLE policy_building_risk (
    policy_record_id NUMBER(19) NOT NULL,
    rated_flood_zone VARCHAR2(20 CHAR),
    flood_zone_current VARCHAR2(20 CHAR),
    base_flood_elevation NUMBER(10,2),
    elevation_difference NUMBER(10),
    location_of_contents NUMBER(5),
    elevation_certificate_indicator VARCHAR2(15 CHAR),
    lowest_floor_elevation NUMBER(10,2),
    lowest_adjacent_grade NUMBER(10,2),
    building_over_water_type NUMBER(5),
    enclosure_type_code VARCHAR2(15 CHAR),
    obstruction_type NUMBER(5),
    mandatory_purchase_flag NUMBER(1),
    post_firm_vzone_indicator NUMBER(1),
    crs_class_code NUMBER(5),
    disaster_assistance_coverage_required_code NUMBER(5),
    building_on_federal_land NUMBER(1),
    foundation_type VARCHAR2(30 CHAR),
    building_replacement_cost NUMBER(20,4),
    elevated_building_indicator NUMBER(1),
    floodproofed_indicator NUMBER(1),
    basement_enclosure_crawlspace_type NUMBER(5),
    pre_firm_indicator NUMBER(1),
    post_firm_construction_indicator NUMBER(1),
    regular_emergency_program_indicator VARCHAR2(15 CHAR),
    CONSTRAINT pk_pol_build_risk PRIMARY KEY (policy_record_id),
    CONSTRAINT fk_pol_build_risk FOREIGN KEY (policy_record_id) REFERENCES policy(policy_record_id)
);

CREATE TABLE policy_contract (
    policy_record_id NUMBER(19) NOT NULL,
    policy_term_indicator NUMBER(5),
    program_type_indicator NUMBER(1),
    policy_effective_date DATE,
    policy_termination_date DATE,
    policy_count NUMBER(10),
    policy_cost NUMBER(20,4),
    building_deductible_code VARCHAR2(15 CHAR),
    contents_deductible_code VARCHAR2(15 CHAR),
    rollover_transfer_code VARCHAR2(15 CHAR),
    waiting_period_type VARCHAR2(15 CHAR),
    total_building_insurance_coverage NUMBER(20,4),
    total_contents_insurance_coverage NUMBER(20,4),
    cancellation_date_of_flood_policy DATE,
    cancellation_voidance_reason_code VARCHAR2(15 CHAR),
    endorsement_effective_date DATE,
    CONSTRAINT pk_pol_contract PRIMARY KEY (policy_record_id),
    CONSTRAINT fk_pol_contract FOREIGN KEY (policy_record_id) REFERENCES policy(policy_record_id),
    CONSTRAINT ck_pol_contract_dates CHECK (policy_termination_date >= policy_effective_date)
);

CREATE TABLE policy_rating (
    policy_record_id NUMBER(19) NOT NULL,
    rate_method VARCHAR2(20 CHAR),
    subsidized_rate_type VARCHAR2(30 CHAR),
    grandfathering_type_code NUMBER(5),
    insurance_to_value_code NUMBER(5),
    basic_building_rate NUMBER(12,6),
    additional_building_rate NUMBER(12,6),
    basic_contents_rate NUMBER(12,6),
    additional_contents_rate NUMBER(12,6),
    CONSTRAINT pk_pol_rating PRIMARY KEY (policy_record_id),
    CONSTRAINT fk_pol_rating FOREIGN KEY (policy_record_id) REFERENCES policy(policy_record_id)
);

CREATE TABLE policy_premium_financial (
    policy_record_id NUMBER(19) NOT NULL,
    total_insurance_premium_of_the_policy NUMBER(20,4),
    federal_policy_fee NUMBER(20,4),
    hfiaa_surcharge NUMBER(20,4),
    icc_premium NUMBER(20,4),
    reserve_fund_assessment NUMBER(20,4),
    community_probation_surcharge NUMBER(20,4),
    premium_payment_indicator NUMBER(5),
    full_risk_premium NUMBER(20,4),
    CONSTRAINT pk_pol_premium PRIMARY KEY (policy_record_id),
    CONSTRAINT fk_pol_premium FOREIGN KEY (policy_record_id) REFERENCES policy(policy_record_id)
);

CREATE INDEX ix_policy_holder ON policy(holder_context);
CREATE INDEX ix_policy_effective ON policy_contract(policy_effective_date);
CREATE INDEX ix_claim_date_of_loss ON claim(date_of_loss);


SELECT table_name FROM user_tables WHERE table_name IN
('HOLDER','PRIVATE_CLIENT','POLICY','CLAIM','POLICY_PROPERTY_LOCATION',
'POLICY_HOLDER_CLASSIFICATION','POLICY_PROPERTY_USAGE',
'POLICY_BUILDING_TECHNICAL','POLICY_BUILDING_RISK','POLICY_CONTRACT',
'POLICY_RATING','POLICY_PREMIUM_FINANCIAL') ORDER BY table_name;