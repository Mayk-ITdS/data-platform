

IF OBJECT_ID('dbo.Holder_Staging','U') IS NULL
BEGIN
    CREATE TABLE dbo.Holder_Staging(
        HolderContext char(64) NOT NULL,
        HolderType VARCHAR(30) NOT NULL
    );
END;