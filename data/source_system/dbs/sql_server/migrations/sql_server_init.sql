
CREATE TABLE dbo.Holder (
    HolderId BIGINT IDENTITY(1,1) NOT NULL,
    HolderContext CHAR(64) NOT NULL,
    HolderType VARCHAR(30) NOT NULL,

    CONSTRAINT PK_Holder PRIMARY KEY (HolderId),
    CONSTRAINT UQ_HolderContext UNIQUE (HolderContext)
);
GO

CREATE TABLE dbo.PrivateClient (
    HolderId BIGINT NOT NULL,
    FullName NVARCHAR(150) NULL,
    BirthYear SMALLINT NULL,
    Sex CHAR(1) NULL,
    Email VARCHAR(254) NULL,
    PhoneNumber VARCHAR(20) NULL,
    Address NVARCHAR(255) NULL,
    State VARCHAR(12) NOT NULL,
    HolderStatus NVARCHAR(30) NULL,
    LoyaltyScore INT NULL,
    CreatedAt DATETIME2(3) NOT NULL
        DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_PrivateClient PRIMARY KEY (HolderId),
    CONSTRAINT FK_PrivateClient_Holder
        FOREIGN KEY (HolderId)
        REFERENCES dbo.Holder(HolderId),

    CONSTRAINT CK_PrivateClient_Sex
        CHECK (Sex IN ('F', 'M'))
);
GO

CREATE TABLE dbo.Policy (
    PolicyId BIGINT NOT NULL,
    HolderId BIGINT NOT NULL,

    CONSTRAINT PK_Policy PRIMARY KEY (PolicyId),

    CONSTRAINT FK_Policy_Holder
        FOREIGN KEY (HolderId)
        REFERENCES dbo.Holder(HolderId)
);
GO

CREATE TABLE dbo.PolicyTerms (
    PolicyTermsId BIGINT IDENTITY(1,1) NOT NULL,
    policy_row_id BIGINT NULL,

    PolicyTermIndicator SMALLINT NULL,
    ProgramTypeIndicator BIT NULL,
    PolicyEffectiveDate DATE NOT NULL,
    PolicyTerminationDate DATE NOT NULL,
    PolicyCount INT NULL,
    PolicyCost DECIMAL(20,4) NULL,
    BuildingDeductibleCode VARCHAR(50) NULL,
    ContentsDeductibleCode VARCHAR(50) NULL,
    RolloverTransferCode VARCHAR(50) NULL,
    WaitingPeriodType VARCHAR(50) NULL,
    TotalBuildingInsuranceCoverage DECIMAL(20,4) NULL,
    TotalContentsInsuranceCoverage DECIMAL(20,4) NULL,
    CancellationDateOfFloodPolicy DATE NULL,
    CancellationVoidanceReasonCode VARCHAR(50) NULL,
    EndorsementEffectiveDate DATE NULL,

    CONSTRAINT PK_PolicyTerms PRIMARY KEY (PolicyTermsId)
);
GO

CREATE TABLE dbo.Property (
    PropertyId BIGINT IDENTITY(1,1) NOT NULL,
    policy_row_id BIGINT NULL,
    ReportedCity VARCHAR(150) NULL,
    ReportedZipCode VARCHAR(20) NULL,
    CensusGeoid VARCHAR(30) NULL,
    Latitude DECIMAL(9,1) NULL,
    Longitude DECIMAL(9,1) NULL,
    FemaRegion SMALLINT NULL,
    PropertyState VARCHAR(12) NULL,
    NfipCommunityName VARCHAR(255) NULL,
    NfipCommunityNumberCurrent VARCHAR(50) NULL,
    NfipRatedCommunityNumber VARCHAR(50) NULL,
    MapPanelNumber VARCHAR(50) NULL,
    MapPanelSuffix VARCHAR(20) NULL,

    CONSTRAINT PK_Property PRIMARY KEY (PropertyId)
);
GO

CREATE TABLE dbo.Building (
    BuildingId BIGINT IDENTITY(1,1) NOT NULL,
    policy_row_id BIGINT NULL,

    OriginalConstructionDate DATE NULL,
    BuildingDescriptionCode SMALLINT NULL,
    BuildingCategory VARCHAR(100) NULL,
    NumberOfFloorsInInsuredBuilding SMALLINT NULL,
    Construction BIT NULL,
    PropertyPurchaseDate DATE NULL,

    CONSTRAINT PK_Building PRIMARY KEY (BuildingId)
);
GO

CREATE TABLE dbo.HolderClassification (
    HolderClassificationId BIGINT IDENTITY(1,1) NOT NULL,
    policy_row_id BIGINT NULL,
    StateOwnedIndicator BIT NULL,
    NonProfitIndicator BIT NULL,
    HouseOfWorshipIndicator BIT NULL,
    SmallBusinessIndicatorBuilding BIT NULL,
    CondominiumCoverageTypeCode VARCHAR(50) NULL,
    CondoCoverageCategory VARCHAR(100) NULL,
    CoverageSignal VARCHAR(100) NULL,

    CONSTRAINT PK_HolderClassification
        PRIMARY KEY (HolderClassificationId)
);
GO

CREATE TABLE dbo.PropertyUsage (
    PropertyUsageId BIGINT IDENTITY(1,1) NOT NULL,
    policy_row_id BIGINT NULL,

    OccupancyType SMALLINT NULL,
    OccupancyCategory VARCHAR(100) NULL,
    PrimaryResidenceIndicator BIT NULL,
    RentalPropertyIndicator BIT NULL,
    TenantIndicator BIT NULL,
    SeasonallyOccupied BIT NULL,
    BuildingPurpose VARCHAR(100) NULL,
    AgricultureStructureIndicator BIT NULL,

    CONSTRAINT PK_PropertyUsage PRIMARY KEY (PropertyUsageId)
);
GO

CREATE TABLE dbo.Risks (
    RiskId BIGINT IDENTITY(1,1) NOT NULL,
    policy_row_id BIGINT NULL,

    RatedFloodZone VARCHAR(50) NULL,
    FloodZoneCurrent VARCHAR(50) NULL,
    BaseFloodElevation DECIMAL(10,2) NULL,
    ElevationDifference INT NULL,
    LocationOfContents SMALLINT NULL,
    ElevationCertificateIndicator VARCHAR(50) NULL,
    LowestFloorElevation DECIMAL(10,2) NULL,
    LowestAdjacentGrade DECIMAL(10,2) NULL,
    BuildingOverWaterType SMALLINT NULL,
    EnclosureTypeCode VARCHAR(50) NULL,
    ObstructionType SMALLINT NULL,
    MandatoryPurchaseFlag BIT NULL,
    PostFirmVzoneIndicator BIT NULL,
    CrsClassCode SMALLINT NULL,
    DisasterAssistanceCoverageRequiredCode SMALLINT NULL,
    BuildingOnFederalLand BIT NULL,
    FoundationType VARCHAR(100) NULL,
    BuildingReplacementCost DECIMAL(20,4) NULL,
    ElevatedBuildingIndicator BIT NULL,
    FloodproofedIndicator BIT NULL,
    BasementEnclosureCrawlspaceType SMALLINT NULL,
    PreFIRMIndicator BIT NULL,
    PostFIRMConstructionIndicator BIT NULL,
    RegularEmergencyProgramIndicator VARCHAR(50) NULL,

    CONSTRAINT PK_Risks PRIMARY KEY (RiskId)
);
GO


CREATE TABLE dbo.PolicyRating (
    PolicyRatingId BIGINT IDENTITY(1,1) NOT NULL,
    policy_row_id BIGINT NULL,

    RateMethod VARCHAR(100) NULL,
    SubsidizedRateType VARCHAR(100) NULL,
    GrandfatheringTypeCode SMALLINT NULL,
    InsuranceToValueCode SMALLINT NULL,
    BasicBuildingRate DECIMAL(12,6) NULL,
    AdditionalBuildingRate DECIMAL(12,6) NULL,
    BasicContentsRate DECIMAL(12,6) NULL,
    AdditionalContentsRate DECIMAL(12,6) NULL,

    CONSTRAINT PK_PolicyRating PRIMARY KEY (PolicyRatingId)
);
GO

CREATE TABLE dbo.PremiumFinancial (
    PremiumFinancialId BIGINT IDENTITY(1,1) NOT NULL,
    policy_row_id BIGINT NULL,

    TotalInsurancePremiumOfThePolicy DECIMAL(20,4) NULL,
    FederalPolicyFee DECIMAL(20,4) NULL,
    HfiaaSurcharge DECIMAL(20,4) NULL,
    IccPremium DECIMAL(20,4) NULL,
    ReserveFundAssessment DECIMAL(20,4) NULL,
    CommunityProbationSurcharge DECIMAL(20,4) NULL,
    PremiumPaymentIndicator SMALLINT NULL,
    FullRiskPremium DECIMAL(20,4) NULL,

    CONSTRAINT PK_PremiumFinancial
        PRIMARY KEY (PremiumFinancialId)
);
GO

CREATE TABLE dbo.PolicyUnit (
    PolicyUnitId BIGINT IDENTITY(1,1) NOT NULL,
    PolicyId BIGINT NOT NULL,
    PropertyId BIGINT NULL,
    BuildingId BIGINT NULL,
    RiskId BIGINT NULL,
    PropertyUsageId BIGINT NULL,
    HolderClassificationId BIGINT NULL,
    PolicyTermsId BIGINT NULL,
    PolicyRatingId BIGINT NULL,
    PremiumFinancialId BIGINT NULL,

    CONSTRAINT PK_PolicyUnit
        PRIMARY KEY (PolicyUnitId),

    CONSTRAINT FK_PolicyUnit_Policy
        FOREIGN KEY (PolicyId)
        REFERENCES dbo.Policy(PolicyId),

    CONSTRAINT FK_PolicyUnit_Property
        FOREIGN KEY (PropertyId)
        REFERENCES dbo.Property(PropertyId),

    CONSTRAINT FK_PolicyUnit_Building
        FOREIGN KEY (BuildingId)
        REFERENCES dbo.Building(BuildingId),

    CONSTRAINT FK_PolicyUnit_Risks
        FOREIGN KEY (RiskId)
        REFERENCES dbo.Risks(RiskId),

    CONSTRAINT FK_PolicyUnit_PropertyUsage
        FOREIGN KEY (PropertyUsageId)
        REFERENCES dbo.PropertyUsage(PropertyUsageId),

    CONSTRAINT FK_PolicyUnit_HolderClassification
        FOREIGN KEY (HolderClassificationId)
        REFERENCES dbo.HolderClassification(HolderClassificationId),

    CONSTRAINT FK_PolicyUnit_PolicyTerms
        FOREIGN KEY (PolicyTermsId)
        REFERENCES dbo.PolicyTerms(PolicyTermsId),

    CONSTRAINT FK_PolicyUnit_PolicyRating
        FOREIGN KEY (PolicyRatingId)
        REFERENCES dbo.PolicyRating(PolicyRatingId),

    CONSTRAINT FK_PolicyUnit_PremiumFinancial
        FOREIGN KEY (PremiumFinancialId)
        REFERENCES dbo.PremiumFinancial(PremiumFinancialId)
);
GO
