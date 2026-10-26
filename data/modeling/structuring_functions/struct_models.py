from pyspark.sql import functions as F


def build_property_location_context_struct():
    return F.struct(
        F.col('reportedCity'),
        F.col('reportedZipCode'),
        F.col('censusGeoid'),
        F.col('latitude'),
        F.col('longitude'),
        F.col('femaRegion'),
        F.col('nfipCommunityName'),
        F.col('nfipCommunityNumberCurrent'),
        F.col('nfipRatedCommunityNumber'),
        F.col('mapPanelNumber'),
        F.col('mapPanelSuffix'),
    ).alias('property_location_context')

def build_holder_classification_context_struct():
    return F.struct(
        F.col('stateOwnedIndicator'),
        F.col('nonProfitIndicator'),
        F.col('houseOfWorshipIndicator'),
        F.col('smallBusinessIndicatorBuilding'),
        F.col('condominiumCoverageTypeCode'),
        F.col('condo_coverage_category'),
        F.col('coverage_signal')
    ).alias('holder_classification_context')

def build_property_usage_struct():
    return F.struct(
        F.col('occupancyType'),
        F.col('occupancy_category'),
        F.col('primaryResidenceIndicator'),
        F.col('rentalPropertyIndicator'),
        F.col('tenantIndicator'),
        F.col('seasonallyOccupied'),
        F.col('buildingPurpose')
    ).alias('property_usage')

def build_building_technical_properties_struct():
    return F.struct(
        F.col('originalConstructionDate'),
        F.col('buildingDescriptionCode'),
        F.col('building_category'),
        F.col('numberOfFloorsInInsuredBuilding'),
        F.col('construction')
    ).alias('building_technical_properties')

def build_building_risk_context_struct():
    return F.struct(
        F.col('ratedFloodZone'),
        F.col('floodZoneCurrent'),
        F.col('baseFloodElevation'),
        F.col('elevationDifference'),
        F.col('locationOfContents'),
        F.col('elevationCertificateIndicator'),
        F.col('lowestFloorElevation'),
        F.col('lowestAdjacentGrade'),
        F.col('buildingOverWaterType'),
        F.col('enclosureTypeCode'),
        F.col('obstructionType'),
        F.col('mandatoryPurchaseFlag'),
        F.col('postFirmVzoneIndicator'),
        F.col('crsClassCode'),
        F.col('disasterAssistanceCoverageRequiredCode'),
        F.col('buildingOnFederalLand'),
        F.col('foundationType'),
        F.col('buildingReplacementCost'),
        F.col('elevatedBuildingIndicator'),
        F.col('floodproofedIndicator'),
        F.col('basementEnclosureCrawlspaceType'),
        F.col('preFIRMIndicator'),
        F.col('postFIRMConstructionIndicator'),
        F.col('regularEmergencyProgramIndicator')
    ).alias('building_risk_context')

def build_policy_contract_context_struct():
    return F.struct(
        F.col('policyTermIndicator'),
        F.col('programTypeIndicator'),
        F.col('policyEffectiveDate'),
        F.col('policyTerminationDate'),
        F.col('policyCount'),
        F.col('policyCost'),
        F.col('buildingDeductibleCode'),
        F.col('contentsDeductibleCode'),
        F.col('rolloverTransferCode'),
        F.col('waitingPeriodType'),
        F.col('totalBuildingInsuranceCoverage'),
        F.col('totalContentsInsuranceCoverage'),
        F.col('cancellationDateOfFloodPolicy'),
        F.col('cancellationVoidanceReasonCode'),
        F.col('endorsementEffectiveDate'),

    ).alias('policy_contract_context')

def build_policy_rating_context_struct():
    return F.struct(
        F.col('rateMethod'),
        F.col('subsidizedRateType'),
        F.col('grandfatheringTypeCode'),
        F.col('insuranceToValueCode'),
        F.col('basicBuildingRate'),
        F.col('additionalBuildingRate'),
        F.col('basicContentsRate'),
        F.col('additionalContentsRate'),
    ).alias('policy_rating_context')

def build_premium_financial_context_struct():
    return F.struct(
        F.col('totalInsurancePremiumOfThePolicy'),
        F.col('federalPolicyFee'),
        F.col('hfiaaSurcharge'),
        F.col('iccPremium'),
        F.col('reserveFundAssessment'),
        F.col('communityProbationSurcharge'),
        F.col('premiumPaymentIndicator'),
        F.col('fullRiskPremium'),
    ).alias('premium_financial_context')