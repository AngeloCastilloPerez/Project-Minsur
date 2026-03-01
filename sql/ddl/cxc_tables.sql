-- ============================================================
-- DDL: CxC (Cuentas por Cobrar) Domain Tables
-- Database: BD_FINANZAS
-- Schema: dbo
-- ============================================================

-- ------------------------------------------------------------
-- EXT_CXC_REAL: External/staging raw data from Azure SQL SAP replica
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.EXT_CXC_REAL', 'U') IS NOT NULL DROP TABLE dbo.EXT_CXC_REAL;
CREATE TABLE dbo.EXT_CXC_REAL (
    ID_EXT_CXC_REAL     BIGINT          IDENTITY(1,1)   NOT NULL,
    BUKRS               VARCHAR(4)      NOT NULL,       -- Company code
    KUNNR               VARCHAR(10)     NOT NULL,       -- Customer number
    BELNR               VARCHAR(10)     NOT NULL,       -- Document number
    BLDAT               VARCHAR(8)      NULL,           -- Document date yyyyMMdd
    FAEDT               VARCHAR(8)      NULL,           -- Due date yyyyMMdd
    DMBTR               DECIMAL(18,2)   NULL,           -- Amount local currency
    WRBTR               DECIMAL(18,2)   NULL,           -- Amount document currency
    WAERS               VARCHAR(5)      NULL,           -- Currency key
    AUGBL               VARCHAR(10)     NULL,           -- Clearing document
    AUGDT               VARCHAR(8)      NULL,           -- Clearing date yyyyMMdd
    ZFBDT               VARCHAR(8)      NULL,           -- Baseline payment date
    KOART               VARCHAR(1)      NULL,           -- Account type (D=Debtor)
    EXECUTION_ID        VARCHAR(100)    NOT NULL,
    FECHA_ACTUALIZACION DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_EXT_CXC_REAL PRIMARY KEY CLUSTERED (ID_EXT_CXC_REAL)
);
CREATE INDEX IX_EXT_CXC_REAL_CLIENTE ON dbo.EXT_CXC_REAL (KUNNR);
CREATE INDEX IX_EXT_CXC_REAL_DOC     ON dbo.EXT_CXC_REAL (BELNR, BUKRS);
GO

-- ------------------------------------------------------------
-- DIM_CXC_CLIENTE: Customer dimension
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.DIM_CXC_CLIENTE', 'U') IS NOT NULL DROP TABLE dbo.DIM_CXC_CLIENTE;
CREATE TABLE dbo.DIM_CXC_CLIENTE (
    ID_CLIENTE          INT             IDENTITY(1,1)   NOT NULL,
    CLIENTE_SAP         VARCHAR(10)     NOT NULL,
    RAZON_SOCIAL        VARCHAR(200)    NOT NULL,
    RUC                 VARCHAR(15)     NULL,
    TIPO_CLIENTE        VARCHAR(50)     NULL,           -- MINERO, COMERCIAL, GOBIERNO, etc.
    PAIS                VARCHAR(3)      NULL,
    CIUDAD              VARCHAR(100)    NULL,
    GRUPO_CLIENTE       VARCHAR(50)     NULL,
    CONDICION_PAGO      VARCHAR(10)     NULL,
    ACTIVO              BIT             NOT NULL DEFAULT 1,
    FECHA_ACTUALIZACION DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_DIM_CXC_CLIENTE PRIMARY KEY CLUSTERED (ID_CLIENTE),
    CONSTRAINT UQ_DIM_CXC_CLIENTE_SAP UNIQUE (CLIENTE_SAP)
);
GO

-- ------------------------------------------------------------
-- FACT_CXC_REAL: Fact table for actual accounts receivable
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.FACT_CXC_REAL', 'U') IS NOT NULL DROP TABLE dbo.FACT_CXC_REAL;
CREATE TABLE dbo.FACT_CXC_REAL (
    ID_FACT_CXC_REAL    BIGINT          IDENTITY(1,1)   NOT NULL,
    EMPRESA             VARCHAR(4)      NOT NULL,
    CLIENTE_SAP         VARCHAR(10)     NOT NULL,
    NRO_DOCUMENTO       VARCHAR(10)     NOT NULL,
    FECHA_DOCUMENTO     DATE            NULL,
    FECHA_VENCIMIENTO   DATE            NULL,
    MONTO_ML            DECIMAL(18,2)   NULL,
    MONTO_USD           DECIMAL(18,2)   NULL,
    MONEDA              VARCHAR(5)      NULL,
    DOC_COMPENSACION    VARCHAR(10)     NULL,
    FECHA_COMPENSACION  DATE            NULL,
    FECHA_BASE_PAGO     DATE            NULL,
    TIPO_CUENTA         VARCHAR(1)      NULL,
    DIAS_VENCIDO        INT             NULL,
    ESTADO              VARCHAR(15)     NULL,           -- ABIERTO / COMPENSADO
    BUCKET_VENCIMIENTO  VARCHAR(20)     NULL,           -- aging bucket
    ANIO_FISCAL         SMALLINT        NOT NULL,
    PERIODO             TINYINT         NOT NULL,
    EXECUTION_ID        VARCHAR(100)    NOT NULL,
    FECHA_PROCESO       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_FACT_CXC_REAL PRIMARY KEY CLUSTERED (ID_FACT_CXC_REAL)
);
CREATE INDEX IX_FACT_CXC_REAL_CLIENTE ON dbo.FACT_CXC_REAL (CLIENTE_SAP);
CREATE INDEX IX_FACT_CXC_REAL_PERIOD  ON dbo.FACT_CXC_REAL (ANIO_FISCAL, PERIODO);
CREATE INDEX IX_FACT_CXC_REAL_ESTADO  ON dbo.FACT_CXC_REAL (ESTADO, BUCKET_VENCIMIENTO);
GO

-- ------------------------------------------------------------
-- FACT_CXC_EXPORTABLE: Aggregated CxC for Power BI
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.FACT_CXC_EXPORTABLE', 'U') IS NOT NULL DROP TABLE dbo.FACT_CXC_EXPORTABLE;
CREATE TABLE dbo.FACT_CXC_EXPORTABLE (
    ID_FACT_CXC_EXP     BIGINT          IDENTITY(1,1)   NOT NULL,
    EMPRESA             VARCHAR(4)      NOT NULL,
    CLIENTE_SAP         VARCHAR(10)     NOT NULL,
    ANIO_FISCAL         SMALLINT        NOT NULL,
    PERIODO             TINYINT         NOT NULL,
    ESTADO              VARCHAR(15)     NULL,
    BUCKET_VENCIMIENTO  VARCHAR(20)     NULL,
    MONEDA              VARCHAR(5)      NULL,
    QTY_DOCUMENTOS      INT             NULL,
    TOTAL_ML            DECIMAL(18,2)   NULL,
    TOTAL_USD           DECIMAL(18,2)   NULL,
    PROMEDIO_DIAS_VENCIDO DECIMAL(10,1) NULL,
    MAX_DIAS_VENCIDO    INT             NULL,
    PRIMERA_FACTURA     DATE            NULL,
    ULTIMA_FACTURA      DATE            NULL,
    EXECUTION_ID        VARCHAR(100)    NOT NULL,
    FECHA_PROCESO       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_FACT_CXC_EXPORTABLE PRIMARY KEY CLUSTERED (ID_FACT_CXC_EXP)
);
CREATE INDEX IX_FACT_CXC_EXP_PERIOD  ON dbo.FACT_CXC_EXPORTABLE (ANIO_FISCAL, PERIODO);
CREATE INDEX IX_FACT_CXC_EXP_CLIENTE ON dbo.FACT_CXC_EXPORTABLE (CLIENTE_SAP);
GO

-- ------------------------------------------------------------
-- LINK_TABLE_CXC: CxC reference / mapping table
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.LINK_TABLE_CXC', 'U') IS NOT NULL DROP TABLE dbo.LINK_TABLE_CXC;
CREATE TABLE dbo.LINK_TABLE_CXC (
    ID_LINK_CXC         INT             IDENTITY(1,1)   NOT NULL,
    TIPO_MAPEO          VARCHAR(30)     NOT NULL,       -- e.g., 'BUCKET', 'TIPO_CLIENTE'
    CODIGO_ORIGEN       VARCHAR(20)     NOT NULL,
    CODIGO_DESTINO      VARCHAR(20)     NOT NULL,
    DESCRIPCION         VARCHAR(150)    NULL,
    ACTIVO              BIT             NOT NULL DEFAULT 1,
    FECHA_CREACION      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_LINK_TABLE_CXC PRIMARY KEY CLUSTERED (ID_LINK_CXC),
    CONSTRAINT UQ_LINK_CXC UNIQUE (TIPO_MAPEO, CODIGO_ORIGEN)
);
GO
