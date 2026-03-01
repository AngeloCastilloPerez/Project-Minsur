-- ============================================================
-- DDL: G&A (Gastos Administrativos) Domain Tables
-- Database: BD_FINANZAS
-- Schema: dbo
-- ============================================================

-- ------------------------------------------------------------
-- EXT_GA_REAL: External/staging raw data from SAP extraction
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.EXT_GA_REAL', 'U') IS NOT NULL DROP TABLE dbo.EXT_GA_REAL;
CREATE TABLE dbo.EXT_GA_REAL (
    ID_EXT_GA_REAL  BIGINT          IDENTITY(1,1)   NOT NULL,
    EMPRESA         VARCHAR(4)      NOT NULL,
    KOSTL           VARCHAR(10)     NOT NULL,       -- SAP cost center
    SAKNR           VARCHAR(10)     NOT NULL,       -- SAP GL account
    GJAHR           SMALLINT        NOT NULL,       -- Fiscal year
    MONAT           TINYINT         NOT NULL,       -- Period (month 1-12)
    WKGBTR          DECIMAL(18,2)   NOT NULL DEFAULT 0, -- Amount local currency
    WTGBTR          DECIMAL(18,2)   NOT NULL DEFAULT 0, -- Amount USD
    VERSN           VARCHAR(3)      NULL,           -- Version (0=actual)
    WRTTP           VARCHAR(2)      NULL,           -- Value type (04=actual, 01=plan)
    EXECUTION_ID    VARCHAR(100)    NOT NULL,
    FECHA_CARGA     DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_EXT_GA_REAL PRIMARY KEY CLUSTERED (ID_EXT_GA_REAL)
);
CREATE INDEX IX_EXT_GA_REAL_PERIOD ON dbo.EXT_GA_REAL (GJAHR, MONAT, EXECUTION_ID);
GO

-- ------------------------------------------------------------
-- DIM_GA_CENTRO_COSTO: Cost center dimension
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.DIM_GA_CENTRO_COSTO', 'U') IS NOT NULL DROP TABLE dbo.DIM_GA_CENTRO_COSTO;
CREATE TABLE dbo.DIM_GA_CENTRO_COSTO (
    ID_CC               INT             IDENTITY(1,1)   NOT NULL,
    CENTRO_COSTO_SAP    VARCHAR(10)     NOT NULL,
    CC_LOCAL            VARCHAR(20)     NOT NULL,
    CC_DESCRIPCION      VARCHAR(100)    NOT NULL,
    CC_GERENCIA         VARCHAR(100)    NULL,
    CC_AREA             VARCHAR(100)    NULL,
    EMPRESA             VARCHAR(4)      NULL,
    ACTIVO              BIT             NOT NULL DEFAULT 1,
    FECHA_INICIO        DATE            NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    FECHA_FIN           DATE            NULL,
    FECHA_ACTUALIZACION DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_DIM_GA_CC PRIMARY KEY CLUSTERED (ID_CC),
    CONSTRAINT UQ_DIM_GA_CC_SAP UNIQUE (CENTRO_COSTO_SAP)
);
GO

-- ------------------------------------------------------------
-- DIM_GA_CUENTA_GL: GL account dimension
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.DIM_GA_CUENTA_GL', 'U') IS NOT NULL DROP TABLE dbo.DIM_GA_CUENTA_GL;
CREATE TABLE dbo.DIM_GA_CUENTA_GL (
    ID_GL               INT             IDENTITY(1,1)   NOT NULL,
    CUENTA_GL_SAP       VARCHAR(10)     NOT NULL,
    GL_LOCAL            VARCHAR(20)     NOT NULL,
    GL_DESCRIPCION      VARCHAR(150)    NOT NULL,
    GL_TIPO_GASTO       VARCHAR(50)     NULL,       -- e.g., PERSONAL, SERVICIOS, MATERIALES
    GL_CLASIFICACION    VARCHAR(50)     NULL,       -- e.g., FIJO, VARIABLE
    ACTIVO              BIT             NOT NULL DEFAULT 1,
    FECHA_ACTUALIZACION DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_DIM_GA_GL PRIMARY KEY CLUSTERED (ID_GL),
    CONSTRAINT UQ_DIM_GA_GL_SAP UNIQUE (CUENTA_GL_SAP)
);
GO

-- ------------------------------------------------------------
-- FACT_GA_REAL: Fact table for actual G&A data
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.FACT_GA_REAL', 'U') IS NOT NULL DROP TABLE dbo.FACT_GA_REAL;
CREATE TABLE dbo.FACT_GA_REAL (
    ID_FACT_GA_REAL     BIGINT          IDENTITY(1,1)   NOT NULL,
    EMPRESA             VARCHAR(4)      NOT NULL,
    CENTRO_COSTO_SAP    VARCHAR(10)     NOT NULL,
    CC_LOCAL            VARCHAR(20)     NULL,
    CC_DESCRIPCION      VARCHAR(100)    NULL,
    CC_GERENCIA         VARCHAR(100)    NULL,
    CC_AREA             VARCHAR(100)    NULL,
    CUENTA_GL_SAP       VARCHAR(10)     NOT NULL,
    GL_LOCAL            VARCHAR(20)     NULL,
    GL_DESCRIPCION      VARCHAR(150)    NULL,
    GL_TIPO_GASTO       VARCHAR(50)     NULL,
    GL_CLASIFICACION    VARCHAR(50)     NULL,
    ANIO_FISCAL         SMALLINT        NOT NULL,
    PERIODO             TINYINT         NOT NULL,
    PERIODO_LABEL       VARCHAR(7)      NOT NULL,   -- e.g., 2024-03
    MONTO_ML            DECIMAL(18,2)   NOT NULL DEFAULT 0,
    MONTO_USD           DECIMAL(18,2)   NOT NULL DEFAULT 0,
    EXECUTION_ID        VARCHAR(100)    NOT NULL,
    FECHA_PROCESO       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_FACT_GA_REAL PRIMARY KEY CLUSTERED (ID_FACT_GA_REAL)
);
CREATE INDEX IX_FACT_GA_REAL_PERIOD ON dbo.FACT_GA_REAL (ANIO_FISCAL, PERIODO);
CREATE INDEX IX_FACT_GA_REAL_CC     ON dbo.FACT_GA_REAL (CENTRO_COSTO_SAP);
CREATE INDEX IX_FACT_GA_REAL_GL     ON dbo.FACT_GA_REAL (CUENTA_GL_SAP);
GO

-- ------------------------------------------------------------
-- FACT_GA_PLAN: Fact table for budget/plan G&A data
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.FACT_GA_PLAN', 'U') IS NOT NULL DROP TABLE dbo.FACT_GA_PLAN;
CREATE TABLE dbo.FACT_GA_PLAN (
    ID_FACT_GA_PLAN     BIGINT          IDENTITY(1,1)   NOT NULL,
    EMPRESA             VARCHAR(4)      NOT NULL,
    CENTRO_COSTO_SAP    VARCHAR(10)     NOT NULL,
    CC_LOCAL            VARCHAR(20)     NULL,
    CC_DESCRIPCION      VARCHAR(100)    NULL,
    CC_GERENCIA         VARCHAR(100)    NULL,
    CC_AREA             VARCHAR(100)    NULL,
    CUENTA_GL_SAP       VARCHAR(10)     NOT NULL,
    GL_LOCAL            VARCHAR(20)     NULL,
    GL_DESCRIPCION      VARCHAR(150)    NULL,
    GL_TIPO_GASTO       VARCHAR(50)     NULL,
    GL_CLASIFICACION    VARCHAR(50)     NULL,
    ANIO_FISCAL         SMALLINT        NOT NULL,
    PERIODO             TINYINT         NOT NULL,
    PERIODO_LABEL       VARCHAR(7)      NOT NULL,
    MONTO_ML_PLAN       DECIMAL(18,2)   NOT NULL DEFAULT 0,
    MONTO_USD_PLAN      DECIMAL(18,2)   NOT NULL DEFAULT 0,
    EXECUTION_ID        VARCHAR(100)    NOT NULL,
    FECHA_PROCESO       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_FACT_GA_PLAN PRIMARY KEY CLUSTERED (ID_FACT_GA_PLAN)
);
CREATE INDEX IX_FACT_GA_PLAN_PERIOD ON dbo.FACT_GA_PLAN (ANIO_FISCAL, PERIODO);
GO

-- ------------------------------------------------------------
-- FACT_GA_EXPORTABLE: Combined real + plan for Power BI
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.FACT_GA_EXPORTABLE', 'U') IS NOT NULL DROP TABLE dbo.FACT_GA_EXPORTABLE;
CREATE TABLE dbo.FACT_GA_EXPORTABLE (
    ID_FACT_GA_EXP      BIGINT          IDENTITY(1,1)   NOT NULL,
    EMPRESA             VARCHAR(4)      NOT NULL,
    CENTRO_COSTO_SAP    VARCHAR(10)     NOT NULL,
    CC_LOCAL            VARCHAR(20)     NULL,
    CC_DESCRIPCION      VARCHAR(100)    NULL,
    CC_GERENCIA         VARCHAR(100)    NULL,
    CC_AREA             VARCHAR(100)    NULL,
    CUENTA_GL_SAP       VARCHAR(10)     NOT NULL,
    GL_LOCAL            VARCHAR(20)     NULL,
    GL_DESCRIPCION      VARCHAR(150)    NULL,
    GL_TIPO_GASTO       VARCHAR(50)     NULL,
    GL_CLASIFICACION    VARCHAR(50)     NULL,
    ANIO_FISCAL         SMALLINT        NOT NULL,
    PERIODO             TINYINT         NOT NULL,
    PERIODO_LABEL       VARCHAR(7)      NOT NULL,
    MONTO_ML_REAL       DECIMAL(18,2)   NOT NULL DEFAULT 0,
    MONTO_USD_REAL      DECIMAL(18,2)   NOT NULL DEFAULT 0,
    MONTO_ML_PLAN       DECIMAL(18,2)   NOT NULL DEFAULT 0,
    MONTO_USD_PLAN      DECIMAL(18,2)   NOT NULL DEFAULT 0,
    VAR_ML              DECIMAL(18,2)   NULL,       -- Real - Plan local currency
    VAR_USD             DECIMAL(18,2)   NULL,       -- Real - Plan USD
    VAR_PCT_ML          DECIMAL(10,2)   NULL,       -- Variance %
    EXECUTION_ID        VARCHAR(100)    NOT NULL,
    FECHA_PROCESO       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_FACT_GA_EXPORTABLE PRIMARY KEY CLUSTERED (ID_FACT_GA_EXP)
);
CREATE INDEX IX_FACT_GA_EXP_PERIOD ON dbo.FACT_GA_EXPORTABLE (ANIO_FISCAL, PERIODO);
GO

-- ------------------------------------------------------------
-- LINK_TABLE_GA_HOMOLOGACION: Homologation mapping table
-- ------------------------------------------------------------
IF OBJECT_ID('dbo.LINK_TABLE_GA_HOMOLOGACION', 'U') IS NOT NULL DROP TABLE dbo.LINK_TABLE_GA_HOMOLOGACION;
CREATE TABLE dbo.LINK_TABLE_GA_HOMOLOGACION (
    ID_HOMOLOGACION     INT             IDENTITY(1,1)   NOT NULL,
    TIPO_HOMOLOGACION   VARCHAR(20)     NOT NULL,       -- 'CENTRO_COSTO' or 'CUENTA_GL'
    CODIGO_SAP          VARCHAR(10)     NOT NULL,
    CODIGO_LOCAL        VARCHAR(20)     NOT NULL,
    DESCRIPCION         VARCHAR(150)    NULL,
    CAMPO_EXTRA_1       VARCHAR(100)    NULL,           -- gerencia for CC, tipo_gasto for GL
    CAMPO_EXTRA_2       VARCHAR(100)    NULL,           -- area for CC, clasificacion for GL
    ACTIVO              BIT             NOT NULL DEFAULT 1,
    FECHA_VIGENCIA      DATE            NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    FECHA_FIN           DATE            NULL,
    USUARIO_CREACION    VARCHAR(50)     NULL,
    FECHA_CREACION      DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    USUARIO_MODIFICACION VARCHAR(50)   NULL,
    FECHA_MODIFICACION  DATETIME2       NULL,
    CONSTRAINT PK_LINK_GA_HOMOLOGACION PRIMARY KEY CLUSTERED (ID_HOMOLOGACION),
    CONSTRAINT UQ_LINK_GA_HOMO UNIQUE (TIPO_HOMOLOGACION, CODIGO_SAP, FECHA_VIGENCIA)
);
GO
