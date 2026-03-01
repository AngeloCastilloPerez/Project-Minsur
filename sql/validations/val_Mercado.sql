-- ============================================================
-- Validation Queries: Mercado (Market) Domain
-- ============================================================

-- ------------------------------------------------------------
-- 1. Row count checks
-- ------------------------------------------------------------
SELECT
    'FACT_MERCADO_REAL'         AS tabla,
    COUNT(*)                    AS total_registros,
    MAX(FECHA_PROCESO)          AS ultima_carga,
    MAX(EXECUTION_ID)           AS ultimo_execution_id
FROM dbo.FACT_MERCADO_REAL;

SELECT
    'FACT_MERCADO_EXPORTABLE'   AS tabla,
    COUNT(*)                    AS total_registros,
    MAX(FECHA_PROCESO)          AS ultima_carga,
    MAX(EXECUTION_ID)           AS ultimo_execution_id
FROM dbo.FACT_MERCADO_EXPORTABLE;
GO

-- ------------------------------------------------------------
-- 2. Null checks on key columns
-- ------------------------------------------------------------
SELECT
    'NULL_EMPRESA'          AS check_name, COUNT(*) AS null_count
FROM dbo.FACT_MERCADO_REAL WHERE EMPRESA IS NULL
UNION ALL
SELECT 'NULL_PRODUCTO_COD', COUNT(*) FROM dbo.FACT_MERCADO_REAL WHERE PRODUCTO_COD IS NULL
UNION ALL
SELECT 'NULL_METAL',        COUNT(*) FROM dbo.FACT_MERCADO_REAL WHERE METAL IS NULL
UNION ALL
SELECT 'NULL_VOLUMEN_REAL', COUNT(*) FROM dbo.FACT_MERCADO_REAL WHERE VOLUMEN_REAL IS NULL
UNION ALL
SELECT 'NULL_PRECIO_USD',   COUNT(*) FROM dbo.FACT_MERCADO_REAL WHERE PRECIO_REAL_USD IS NULL
UNION ALL
SELECT 'NULL_INGRESO_USD',  COUNT(*) FROM dbo.FACT_MERCADO_REAL WHERE INGRESO_REAL_USD IS NULL;
GO

-- ------------------------------------------------------------
-- 3. Ingreso reconciliation: Volumen * Precio vs Ingreso declarado
-- ------------------------------------------------------------
SELECT
    EMPRESA,
    PRODUCTO_COD,
    ANIO_FISCAL,
    PERIODO,
    VOLUMEN_REAL,
    PRECIO_REAL_USD,
    INGRESO_REAL_USD,
    INGRESO_CALCULADO_USD,
    ABS(INGRESO_REAL_USD - INGRESO_CALCULADO_USD) AS DIFERENCIA_USD,
    CASE
        WHEN ABS(INGRESO_REAL_USD - INGRESO_CALCULADO_USD) > 100 THEN 'REVISAR'
        ELSE 'OK'
    END AS ESTADO_RECONCILIACION
FROM dbo.FACT_MERCADO_REAL
WHERE INGRESO_CALCULADO_USD IS NOT NULL
  AND ABS(INGRESO_REAL_USD - INGRESO_CALCULADO_USD) > 1  -- tolerance of 1 USD
ORDER BY DIFERENCIA_USD DESC;
GO

-- ------------------------------------------------------------
-- 4. Sum reconciliation: EXT vs FACT
-- ------------------------------------------------------------
DECLARE @exec_id VARCHAR(100) = (SELECT TOP 1 EXECUTION_ID FROM dbo.FACT_MERCADO_REAL ORDER BY FECHA_PROCESO DESC);

SELECT
    'EXT_MERCADO_REAL'      AS fuente,
    SUM(INGRESO_REAL_ML)    AS total_ingreso_ml,
    SUM(INGRESO_REAL_USD)   AS total_ingreso_usd,
    SUM(VOLUMEN_REAL)       AS total_volumen,
    COUNT(*)                AS registros
FROM dbo.EXT_MERCADO_REAL
WHERE EXECUTION_ID = @exec_id

UNION ALL

SELECT
    'FACT_MERCADO_REAL'     AS fuente,
    SUM(INGRESO_REAL_ML)    AS total_ingreso_ml,
    SUM(INGRESO_REAL_USD)   AS total_ingreso_usd,
    SUM(VOLUMEN_REAL)       AS total_volumen,
    COUNT(*)                AS registros
FROM dbo.FACT_MERCADO_REAL
WHERE EXECUTION_ID = @exec_id;
GO

-- ------------------------------------------------------------
-- 5. Duplicate detection
-- ------------------------------------------------------------
SELECT
    EMPRESA,
    PRODUCTO_COD,
    ANIO_FISCAL,
    PERIODO,
    EXECUTION_ID,
    COUNT(*) AS duplicados
FROM dbo.FACT_MERCADO_REAL
GROUP BY EMPRESA, PRODUCTO_COD, ANIO_FISCAL, PERIODO, EXECUTION_ID
HAVING COUNT(*) > 1
ORDER BY duplicados DESC;
GO

-- ------------------------------------------------------------
-- 6. Negative volume or price check
-- ------------------------------------------------------------
SELECT
    EMPRESA,
    PRODUCTO_COD,
    METAL,
    ANIO_FISCAL,
    PERIODO,
    VOLUMEN_REAL,
    PRECIO_REAL_USD,
    INGRESO_REAL_USD
FROM dbo.FACT_MERCADO_REAL
WHERE VOLUMEN_REAL < 0 OR PRECIO_REAL_USD < 0
ORDER BY ANIO_FISCAL DESC, PERIODO DESC;
GO

-- ------------------------------------------------------------
-- 7. Summary by metal for current year
-- ------------------------------------------------------------
SELECT
    METAL,
    ANIO_FISCAL,
    SUM(VOLUMEN_REAL)       AS total_volumen,
    AVG(PRECIO_REAL_USD)    AS precio_promedio_usd,
    SUM(INGRESO_REAL_USD)   AS total_ingreso_usd
FROM dbo.FACT_MERCADO_REAL
WHERE ANIO_FISCAL = YEAR(GETDATE())
GROUP BY METAL, ANIO_FISCAL
ORDER BY total_ingreso_usd DESC;
GO
