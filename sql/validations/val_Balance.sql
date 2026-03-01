-- ============================================================
-- Validation Queries: Balance General Domain
-- ============================================================

-- ------------------------------------------------------------
-- 1. Row count checks
-- ------------------------------------------------------------
SELECT
    'FACT_BALANCE_REAL'     AS tabla,
    COUNT(*)                AS total_registros,
    MAX(FECHA_PROCESO)      AS ultima_carga,
    MAX(EXECUTION_ID)       AS ultimo_execution_id
FROM dbo.FACT_BALANCE_REAL;

SELECT
    'FACT_BALANCE_EXPORTABLE' AS tabla,
    COUNT(*)                AS total_registros,
    MAX(FECHA_PROCESO)      AS ultima_carga,
    MAX(EXECUTION_ID)       AS ultimo_execution_id
FROM dbo.FACT_BALANCE_EXPORTABLE;
GO

-- ------------------------------------------------------------
-- 2. Null checks on key columns
-- ------------------------------------------------------------
SELECT
    'NULL_EMPRESA'          AS check_name, COUNT(*) AS null_count
FROM dbo.FACT_BALANCE_REAL WHERE EMPRESA IS NULL
UNION ALL
SELECT 'NULL_CUENTA_CONTABLE', COUNT(*) FROM dbo.FACT_BALANCE_REAL WHERE CUENTA_CONTABLE IS NULL
UNION ALL
SELECT 'NULL_CLASIFICACION',   COUNT(*) FROM dbo.FACT_BALANCE_REAL WHERE CLASIFICACION IS NULL
UNION ALL
SELECT 'NULL_LADO_BALANCE',    COUNT(*) FROM dbo.FACT_BALANCE_REAL WHERE LADO_BALANCE IS NULL
UNION ALL
SELECT 'NULL_SALDO_ML',        COUNT(*) FROM dbo.FACT_BALANCE_REAL WHERE SALDO_ML IS NULL;
GO

-- ------------------------------------------------------------
-- 3. Balance equation check: ACTIVO = PASIVO + PATRIMONIO
-- ------------------------------------------------------------
SELECT
    EMPRESA,
    ANIO_FISCAL,
    PERIODO,
    SUM(CASE WHEN LADO_BALANCE = 'ACTIVO'     THEN SALDO_ML ELSE 0 END) AS TOTAL_ACTIVO,
    SUM(CASE WHEN LADO_BALANCE = 'PASIVO'     THEN SALDO_ML ELSE 0 END) AS TOTAL_PASIVO,
    SUM(CASE WHEN LADO_BALANCE = 'PATRIMONIO' THEN SALDO_ML ELSE 0 END) AS TOTAL_PATRIMONIO,
    SUM(CASE WHEN LADO_BALANCE = 'ACTIVO'     THEN SALDO_ML ELSE 0 END)
    - SUM(CASE WHEN LADO_BALANCE IN ('PASIVO', 'PATRIMONIO') THEN SALDO_ML ELSE 0 END)
        AS DIFERENCIA_BALANCE    -- Should be 0 for a balanced sheet
FROM dbo.FACT_BALANCE_REAL
GROUP BY EMPRESA, ANIO_FISCAL, PERIODO
ORDER BY ANIO_FISCAL DESC, PERIODO DESC;
GO

-- ------------------------------------------------------------
-- 4. Sum reconciliation: EXT vs FACT
-- ------------------------------------------------------------
DECLARE @exec_id VARCHAR(100) = (SELECT TOP 1 EXECUTION_ID FROM dbo.FACT_BALANCE_REAL ORDER BY FECHA_PROCESO DESC);

SELECT
    'EXT_BALANCE_REAL'  AS fuente,
    SUM(SALDO_ML)       AS total_saldo_ml,
    SUM(SALDO_USD)      AS total_saldo_usd,
    COUNT(*)            AS registros
FROM dbo.EXT_BALANCE_REAL
WHERE EXECUTION_ID = @exec_id

UNION ALL

SELECT
    'FACT_BALANCE_REAL' AS fuente,
    SUM(SALDO_ML)       AS total_saldo_ml,
    SUM(SALDO_USD)      AS total_saldo_usd,
    COUNT(*)            AS registros
FROM dbo.FACT_BALANCE_REAL
WHERE EXECUTION_ID = @exec_id;
GO

-- ------------------------------------------------------------
-- 5. Duplicate detection
-- ------------------------------------------------------------
SELECT
    EMPRESA,
    CUENTA_CONTABLE,
    ANIO_FISCAL,
    PERIODO,
    EXECUTION_ID,
    COUNT(*) AS duplicados
FROM dbo.FACT_BALANCE_REAL
GROUP BY EMPRESA, CUENTA_CONTABLE, ANIO_FISCAL, PERIODO, EXECUTION_ID
HAVING COUNT(*) > 1
ORDER BY duplicados DESC;
GO

-- ------------------------------------------------------------
-- 6. Accounts without classification mapping
-- ------------------------------------------------------------
SELECT
    CUENTA_CONTABLE,
    COUNT(*) AS registros
FROM dbo.FACT_BALANCE_REAL
WHERE CLASIFICACION IS NULL OR LADO_BALANCE = 'OTRO'
GROUP BY CUENTA_CONTABLE
ORDER BY registros DESC;
GO
