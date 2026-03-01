-- ============================================================
-- Validation Queries: CxC (Cuentas por Cobrar) Domain
-- ============================================================

-- ------------------------------------------------------------
-- 1. Row count checks
-- ------------------------------------------------------------
SELECT
    'FACT_CXC_REAL'         AS tabla,
    COUNT(*)                AS total_registros,
    MAX(FECHA_PROCESO)      AS ultima_carga,
    MAX(EXECUTION_ID)       AS ultimo_execution_id
FROM dbo.FACT_CXC_REAL;

SELECT
    'FACT_CXC_EXPORTABLE'   AS tabla,
    COUNT(*)                AS total_registros,
    MAX(FECHA_PROCESO)      AS ultima_carga,
    MAX(EXECUTION_ID)       AS ultimo_execution_id
FROM dbo.FACT_CXC_EXPORTABLE;
GO

-- ------------------------------------------------------------
-- 2. Null checks on key columns
-- ------------------------------------------------------------
SELECT
    'NULL_EMPRESA'          AS check_name, COUNT(*) AS null_count
FROM dbo.FACT_CXC_REAL WHERE EMPRESA IS NULL
UNION ALL
SELECT 'NULL_CLIENTE_SAP',  COUNT(*) FROM dbo.FACT_CXC_REAL WHERE CLIENTE_SAP IS NULL
UNION ALL
SELECT 'NULL_NRO_DOCUMENTO', COUNT(*) FROM dbo.FACT_CXC_REAL WHERE NRO_DOCUMENTO IS NULL
UNION ALL
SELECT 'NULL_FECHA_DOCUMENTO', COUNT(*) FROM dbo.FACT_CXC_REAL WHERE FECHA_DOCUMENTO IS NULL
UNION ALL
SELECT 'NULL_MONTO_ML',     COUNT(*) FROM dbo.FACT_CXC_REAL WHERE MONTO_ML IS NULL
UNION ALL
SELECT 'NULL_ESTADO',       COUNT(*) FROM dbo.FACT_CXC_REAL WHERE ESTADO IS NULL;
GO

-- ------------------------------------------------------------
-- 3. Sum reconciliation: EXT vs FACT
-- ------------------------------------------------------------
DECLARE @exec_id VARCHAR(100) = (SELECT TOP 1 EXECUTION_ID FROM dbo.FACT_CXC_REAL ORDER BY FECHA_PROCESO DESC);

SELECT
    'EXT_CXC_REAL'      AS fuente,
    SUM(DMBTR)          AS total_monto_ml,
    SUM(WRBTR)          AS total_monto_usd,
    COUNT(*)            AS registros
FROM dbo.EXT_CXC_REAL
WHERE EXECUTION_ID = @exec_id

UNION ALL

SELECT
    'FACT_CXC_REAL'     AS fuente,
    SUM(MONTO_ML)       AS total_monto_ml,
    SUM(MONTO_USD)      AS total_monto_usd,
    COUNT(*)            AS registros
FROM dbo.FACT_CXC_REAL
WHERE EXECUTION_ID = @exec_id;
GO

-- ------------------------------------------------------------
-- 4. Duplicate document detection
-- ------------------------------------------------------------
SELECT
    EMPRESA,
    CLIENTE_SAP,
    NRO_DOCUMENTO,
    EXECUTION_ID,
    COUNT(*)            AS duplicados
FROM dbo.FACT_CXC_REAL
GROUP BY EMPRESA, CLIENTE_SAP, NRO_DOCUMENTO, EXECUTION_ID
HAVING COUNT(*) > 1
ORDER BY duplicados DESC;
GO

-- ------------------------------------------------------------
-- 5. Aging distribution check
-- ------------------------------------------------------------
SELECT
    BUCKET_VENCIMIENTO,
    COUNT(*)            AS qty_documentos,
    SUM(MONTO_USD)      AS total_usd,
    AVG(DIAS_VENCIDO)   AS promedio_dias
FROM dbo.FACT_CXC_REAL
WHERE ESTADO = 'ABIERTO'
GROUP BY BUCKET_VENCIMIENTO
ORDER BY
    CASE BUCKET_VENCIMIENTO
        WHEN 'VIGENTE'    THEN 1
        WHEN '1-30 DIAS'  THEN 2
        WHEN '31-60 DIAS' THEN 3
        WHEN '61-90 DIAS' THEN 4
        WHEN '+90 DIAS'   THEN 5
        ELSE 6
    END;
GO

-- ------------------------------------------------------------
-- 6. Negative amounts check (credits/adjustments to validate)
-- ------------------------------------------------------------
SELECT
    EMPRESA,
    CLIENTE_SAP,
    NRO_DOCUMENTO,
    MONTO_ML,
    MONTO_USD,
    FECHA_DOCUMENTO
FROM dbo.FACT_CXC_REAL
WHERE MONTO_ML < 0
ORDER BY MONTO_ML ASC;
GO
