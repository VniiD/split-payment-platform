IF DB_ID(N'SplitPayment') IS NULL
BEGIN
    CREATE DATABASE [SplitPayment];
END;
GO

IF SUSER_ID(N'splitpayment_app') IS NULL
BEGIN
    DECLARE @applicationPassword NVARCHAR(128) = N'$(AppPassword)';
    DECLARE @createLogin NVARCHAR(MAX) =
        N'CREATE LOGIN [splitpayment_app] WITH PASSWORD = ' + QUOTENAME(@applicationPassword, '''') +
        N', CHECK_POLICY = ON;';
    EXEC sys.sp_executesql @createLogin;
END;
GO

USE [SplitPayment];
GO

IF USER_ID(N'splitpayment_app') IS NULL
BEGIN
    CREATE USER [splitpayment_app] FOR LOGIN [splitpayment_app];
END;
GO

IF OBJECT_ID(N'dbo.CgibsTaxLedger', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.CgibsTaxLedger (
        LedgerId UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_CgibsTaxLedger PRIMARY KEY,
        InvoiceId UNIQUEIDENTIFIER NOT NULL,
        MerchantTaxId VARCHAR(14) NOT NULL,
        GrossAmount DECIMAL(18, 4) NOT NULL,
        IbsAmount DECIMAL(28, 12) NOT NULL,
        CbsAmount DECIMAL(28, 12) NOT NULL,
        NetAmount DECIMAL(28, 12) NOT NULL,
        ProcessedAt DATETIME2 NOT NULL,
        Status VARCHAR(20) NOT NULL,
        CONSTRAINT UQ_CgibsTaxLedger_Invoice UNIQUE (InvoiceId),
        CONSTRAINT CK_CgibsTaxLedger_Sum CHECK
            (CONVERT(DECIMAL(28, 12), GrossAmount) = IbsAmount + CbsAmount + NetAmount)
    );

    CREATE INDEX IX_CgibsTaxLedger_Merchant ON dbo.CgibsTaxLedger(MerchantTaxId);
END;
GO

GRANT SELECT, INSERT ON dbo.CgibsTaxLedger TO [splitpayment_app];
GO