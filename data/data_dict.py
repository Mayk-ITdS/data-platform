
all_types_claims = {
    "agricultureStructureIndicator": "boolean",
    "asOfDate": "timestamp",
    "basementEnclosureCrawlspaceType": "smallint",
    "policyCount": "int",
    "crsClassCode": "smallint",
    "dateOfLoss": "date",

    "elevatedBuildingIndicator": "boolean",
    "elevationCertificateIndicator": "string",
    "elevationDifference": "int",

    "baseFloodElevation": "decimal(10,2)",
    "ratedFloodZone": "string",
    "houseWorship": "boolean",
    "locationOfContents": "smallint",

    "lowestAdjacentGrade": "decimal(10,2)",
    "lowestFloorElevation": "decimal(10,2)",
    "numberOfFloorsInTheInsuredBuilding": "smallint",

    "nonProfitIndicator": "boolean",
    "obstructionType": "smallint",
    "occupancyType": "smallint",

    "originalConstructionDate": "date",
    "originalNBDate": "date",

    "amountPaidOnBuildingClaim": "decimal(20,4)",
    "amountPaidOnContentsClaim": "decimal(20,4)",
    "amountPaidOnIncreasedCostOfComplianceClaim": "decimal(20,4)",

    "postFIRMConstructionIndicator": "boolean",
    "rateMethod": "string",
    "smallBusinessIndicatorBuilding": "boolean",

    "totalBuildingInsuranceCoverage": "decimal(20,4)",
    "totalContentsInsuranceCoverage": "decimal(20,4)",

    "yearOfLoss": "smallint",
    "primaryResidenceIndicator": "boolean",

    "buildingDamageAmount": "decimal(20,4)",
    "buildingDeductibleCode": "string",
    "netBuildingPaymentAmount": "decimal(20,4)",
    "buildingPropertyValue": "decimal(20,4)",

    "causeOfDamage": "string",
    "condominiumCoverageTypeCode": "string",

    "contentsDamageAmount": "decimal(20,4)",
    "contentsDeductibleCode": "string",
    "netContentsPaymentAmount": "decimal(20,4)",
    "contentsPropertyValue": "decimal(20,4)",

    "disasterAssistanceCoverageRequired": "smallint",
    "eventDesignationNumber": "string",
    "ficoNumber": "string",

    "floodCharacteristicsIndicator": "int",
    "floodWaterDuration": "int",
    "floodproofedIndicator": "boolean",
    "floodEvent": "string",

    "iccCoverage": "decimal(20,4)",
    "netIccPaymentAmount": "decimal(20,4)",

    "nfipRatedCommunityNumber": "string",
    "nfipCommunityNumberCurrent": "string",
    "nfipCommunityName": "string",

    "nonPaymentReasonContents": "string",
    "nonPaymentReasonBuilding": "string",

    "numberOfUnits": "int",

    "buildingReplacementCost": "decimal(20,4)",
    "contentsReplacementCost": "decimal(20,4)",
    "replacementCostBasis": "string",

    "stateOwnedIndicator": "boolean",

    "waterDepth": "int",
    "floodZoneCurrent": "string",
    "buildingDescriptionCode": "smallint",

    "rentalPropertyIndicator": "boolean",
    "state": "string",
    "reportedCity": "string",
    "reportedZipCode": "string",
    "countyCode": "string",
    "censusGeoid": "string",

    "latitude": "decimal(9,1)",
    "longitude": "decimal(9,1)",

    "foundationType": "string",
    "openDate": "date",
    "mostRecentRecoveryDate": "date",

    "exteriorWaterDepth": "int",
    "interiorWaterDepth": "int",

    "mostRecentPaymentDate": "date",
    "preFirmIndicator": "boolean",

    "totalSalvageRecovery": "decimal(20,4)",
    "totalBldgClaimPmtRecovery": "decimal(20,4)",
    "totalContentsClaimPmtRecovery": "decimal(20,4)",
    "totalIccClaimPmtRecovery": "decimal(20,4)",
    "totalSubrogationRecovery": "decimal(20,4)",

    "id": "bigint",
}
all_types_policies = {
    "agricultureStructureIndicator": "boolean",
    "asOfDate": "timestamp",

    "baseFloodElevation": "decimal(10,2)",
    "basementEnclosureCrawlspaceType": "smallint",

    "cancellationDateOfFloodPolicy": "date",
    "condominiumCoverageTypeCode": "string",
    "construction": "boolean",

    "crsClassCode": "smallint",

    "buildingDeductibleCode": "string",
    "contentsDeductibleCode": "string",

    "elevatedBuildingIndicator": "boolean",
    "elevationCertificateIndicator": "string",
    "elevationDifference": "int",

    "federalPolicyFee": "decimal(20,4)",
    "ratedFloodZone": "string",
    "hfiaaSurcharge": "decimal(20,4)",

    "houseOfWorshipIndicator": "boolean",
    "locationOfContents": "smallint",

    "lowestAdjacentGrade": "decimal(10,2)",
    "lowestFloorElevation": "decimal(10,2)",

    "nonProfitIndicator": "boolean",
    "numberOfFloorsInInsuredBuilding": "smallint",

    "obstructionType": "smallint",
    "occupancyType": "smallint",

    "originalConstructionDate": "date",
    "originalNBDate": "date",

    "policyCost": "decimal(20,4)",
    "policyCount": "int",

    "policyEffectiveDate": "date",
    "policyTerminationDate": "date",
    "policyTermIndicator": "smallint",

    "postFIRMConstructionIndicator": "boolean",
    "primaryResidenceIndicator": "boolean",

    "rateMethod": "string",
    "regularEmergencyProgramIndicator": "string",

    "smallBusinessIndicatorBuilding": "boolean",

    "totalBuildingInsuranceCoverage": "decimal(20,4)",
    "totalContentsInsuranceCoverage": "decimal(20,4)",
    "totalInsurancePremiumOfThePolicy": "decimal(20,4)",

    "cancellationVoidanceReasonCode": "string",
    "subsidizedRateType": "string",

    "iccPremium": "decimal(20,4)",
    "reserveFundAssessment": "decimal(20,4)",
    "communityProbationSurcharge": "decimal(20,4)",

    "premiumPaymentIndicator": "smallint",

    "buildingReplacementCost": "decimal(20,4)",

    "basicBuildingRate": "decimal(12,6)",
    "additionalBuildingRate": "decimal(12,6)",
    "basicContentsRate": "decimal(12,6)",
    "additionalContentsRate": "decimal(12,6)",

    "enclosureTypeCode": "string",
    "buildingDescriptionCode": "smallint",
    "insuranceToValueCode": "smallint",

    "postFirmVzoneIndicator": "boolean",
    "floodproofedIndicator": "boolean",

    "waitingPeriodType": "string",
    "rolloverTransferCode": "string",

    "endorsementEffectiveDate": "date",
    "propertyPurchaseDate": "date",

    "rentalPropertyIndicator": "boolean",
    "tenantIndicator": "boolean",
    "stateOwnedIndicator": "boolean",

    "disasterAssistanceCoverageRequiredCode": "smallint",
    "mandatoryPurchaseFlag": "boolean",
    "grandfatheringTypeCode": "smallint",

    "nfipRatedCommunityNumber": "string",
    "nfipCommunityNumberCurrent": "string",
    "nfipCommunityName": "string",

    "programTypeIndicator": "boolean",

    "mapPanelNumber": "string",
    "mapPanelSuffix": "string",
    "floodZoneCurrent": "string",

    "femaRegion": "smallint",

    "propertyState": "string",
    "reportedCity": "string",
    "reportedZipCode": "string",
    "censusGeoid": "string",

    "latitude": "decimal(9,1)",
    "longitude": "decimal(9,1)",

    "buildingOnFederalLand": "boolean",
    "buildingPurpose": "string",
    "seasonallyOccupied": "boolean",

    "fullRiskPremium": "decimal(20,4)",

    "buildingOverWaterType": "smallint",
    "foundationType": "string",
    "preFIRMIndicator": "boolean",
}

categorical_columns = [x for x,y in all_types_policies.items() if y == "string"]
non_categorical_columns = all_types_policies.keys() - categorical_columns