-- ============================================================
-- Validation Queries: G&A Domain
-- Run after each ETL execution to verify data quality
-- ============================================================

-- ------------------------------------------------------------
-- 1. Row count checks
-- ------------------------------------------------------------

-- Count records loaded in last execution
SELECT
    'FACT_GA_REAL'      AS tabla,
    COUNT(*)            AS total_registros,
    MAX(FECHA_PROCESO)  AS ultima_carga,
    MAX(EXECUTION_ID)   AS ultimo_execution_id
FROM dbo.FACT_GA_REAL;

SELECT
    'FACT_GA_PLAN'      AS tabla,
    COUNT(*)            AS total_registros,
    MAX(FECHA_PROCESO)  AS ultima_carga,
    MAX(EXECUTION_ID)   AS ultimo_execution_id
FROM dbo.FACT_GA_PLAN;

SELECT
    'FACT_GA_EXPORTABLE' AS tabla,
    COUNT(*)             AS total_registros,
    MAX(FECHA_PROCESO)   AS ultima_carga,
    MAX(EXECUTION_ID)    AS ultimo_execution_id
FROM dbo.FACT_GA_EXPORTABLE;
GO

-- ------------------------------------------------------------
-- 2. Null checks on key columns
-- ------------------------------------------------------------

-- Alert if any key dimension columns are NULL in FACT_GA_REAL
SELECT
    'NULL_EMPRESA'          AS check_name,
    COUNT(*)                AS null_count
FROM dbo.FACT_GA_REAL WHERE EMPRESA IS NULL
UNION ALL
SELECT
    'NULL_CENTRO_COSTO_SAP',
    COUNT(*)
FROM dbo.FACT_GA_REAL WHERE CENTRO_COSTO_SAP IS NULL
UNION ALL
SELECT
    'NULL_CUENTA_GL_SAP',
    COUNT(*)
FROM dbo.FACT_GA_REAL WHERE CUENTA_GL_SAP IS NULL
UNION ALL
SELECT
    'NULL_CC_LOCAL',
    COUNT(*)
FROM dbo.FACT_GA_REAL WHERE CC_LOCAL IS NULL
UNION ALL
SELECT
    'NULL_GL_LOCAL',
    COUNT(*)
FROM dbo.FACT_GA_REAL WHERE GL_LOCAL IS NULL
UNION ALL
SELECT
    'NULL_MONTO_ML',
    COUNT(*)
FROM dbo.FACT_GA_REAL WHERE MONTO_ML IS NULL;
GO

-- ------------------------------------------------------------
-- 3. Sum reconciliation: staging vs fact table
-- ------------------------------------------------------------

-- Compare total amounts: EXT_GA_REAL vs FACT_GA_REAL for latest execution
DECLARE @exec_id VARCHAR(100) = (SELECT TOP 1 EXECUTION_ID FROM dbo.FACT_GA_REAL ORDER BY FECHA_PROCESO DESC);

SELECT
    'EXT_GA_REAL'       AS fuente,
    SUM(WKGBTR)         AS total_monto_ml,
    SUM(WTGBTR)         AS total_monto_usd,
    COUNT(*)            AS registros
FROM dbo.EXT_GA_REAL
WHERE EXECUTION_ID = @exec_id

UNION ALL

SELECT
    'FACT_GA_REAL'      AS fuente,
    SUM(MONTO_ML)       AS total_monto_ml,
    SUM(MONTO_USD)      AS total_monto_usd,
    COUNT(*)            AS registros
FROM dbo.FACT_GA_REAL
WHERE EXECUTION_ID = @exec_id;
GO

-- ------------------------------------------------------------
-- 4. Duplicate detection
-- ------------------------------------------------------------

-- Detect duplicate combinations in FACT_GA_REAL
SELECT
    EMPRESA,
    CENTRO_COSTO_SAP,
    CUENTA_GL_SAP,
    ANIO_FISCAL,
    PERIODO,
    EXECUTION_ID,
    COUNT(*)            AS duplicados
FROM dbo.FACT_GA_REAL
GROUP BY EMPRESA, CENTRO_COSTO_SAP, CUENTA_GL_SAP, ANIO_FISCAL, PERIODO, EXECUTION_ID
HAVING COUNT(*) > 1
ORDER BY duplicados DESC;
GO

-- Detect duplicate combinations in FACT_GA_EXPORTABLE
SELECT
    EMPRESA,
    CENTRO_COSTO_SAP,
    CUENTA_GL_SAP,
    ANIO_FISCAL,
    PERIODO,
    EXECUTION_ID,
    COUNT(*)            AS duplicados
FROM dbo.FACT_GA_EXPORTABLE
GROUP BY EMPRESA, CENTRO_COSTO_SAP, CUENTA_GL_SAP, ANIO_FISCAL, PERIODO, EXECUTION_ID
HAVING COUNT(*) > 1
ORDER BY duplicados DESC;
GO

-- ------------------------------------------------------------
-- 5. Real vs Plan variance check: flag periods with >50% variance
-- ------------------------------------------------------------
SELECT
    EMPRESA,
    ANIO_FISCAL,
    PERIODO,
    SUM(MONTO_ML_REAL)  AS total_real_ml,
    SUM(MONTO_ML_PLAN)  AS total_plan_ml,
    SUM(VAR_ML)         AS varianza_ml,
    AVG(VAR_PCT_ML)     AS varianza_pct_promedio
FROM dbo.FACT_GA_EXPORTABLE
GROUP BY EMPRESA, ANIO_FISCAL, PERIODO
HAVING ABS(AVG(VAR_PCT_ML)) > 50
ORDER BY ANIO_FISCAL DESC, PERIODO DESC;
GO

-- ------------------------------------------------------------
-- 6. Unmapped cost centers / GL accounts
-- ------------------------------------------------------------
SELECT DISTINCT
    CENTRO_COSTO_SAP,
    COUNT(*) AS registros
FROM dbo.FACT_GA_REAL
WHERE CC_LOCAL IS NULL
GROUP BY CENTRO_COSTO_SAP
ORDER BY registros DESC;

SELECT DISTINCT
    CUENTA_GL_SAP,
    COUNT(*) AS registros
FROM dbo.FACT_GA_REAL
WHERE GL_LOCAL IS NULL
GROUP BY CUENTA_GL_SAP
ORDER BY registros DESC;
GO
