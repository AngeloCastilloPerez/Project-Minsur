# Databricks notebook source
# COMMAND ----------
# DBTITLE 1, GA - Real: Transformación de datos reales y carga en FACT_GA_REAL

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from datetime import datetime
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nb_GA_02_Real")

# COMMAND ----------
# Parameters via widgets
dbutils.widgets.text("storage_account", "", "Storage Account Name")
dbutils.widgets.text("environment", "dev", "Environment (dev/stg/prd)")
dbutils.widgets.text("execution_id", "", "Execution ID")

storage_account = dbutils.widgets.get("storage_account")
environment     = dbutils.widgets.get("environment")
execution_id    = dbutils.widgets.get("execution_id")

logger.info(f"Starting nb_GA_02_Real | env={environment} | exec_id={execution_id}")

# COMMAND ----------
# ADLS Gen2 connection via Key Vault secret scope
storage_key = dbutils.secrets.get(scope="kv-test", key="test-adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------
# Medallion paths
staging_path = f"abfss://test-staging@{storage_account}.dfs.core.windows.net/ga/"
curated_path = f"abfss://test-curated@{storage_account}.dfs.core.windows.net/ga/"
logs_path    = f"abfss://test-logs@{storage_account}.dfs.core.windows.net/ga/real/"

# COMMAND ----------
# DBTITLE 2, Read homologated data from staging
df_staging = (
    spark.read
    .parquet(f"{staging_path}homologaciones/execution_id={execution_id}/")
    .filter(F.col("FLAG_HOMOLOGADO_CC") == 1)
    .filter(F.col("FLAG_HOMOLOGADO_GL") == 1)
)

logger.info(f"Records from staging: {df_staging.count()}")

# COMMAND ----------
# DBTITLE 3, Transform real financial data
df_real = (
    df_staging
    .withColumn("MONTO_ML",  F.round(F.col("MONTO_ML"),  2))
    .withColumn("MONTO_USD", F.round(F.col("MONTO_USD"), 2))
    # Derive period label for readability
    .withColumn(
        "PERIODO_LABEL",
        F.concat(F.col("ANIO_FISCAL").cast(StringType()), F.lit("-"), F.lpad(F.col("PERIODO").cast(StringType()), 2, "0"))
    )
    # Keep only real (actual) records – filter out plan entries if mixed
    .filter(F.col("EMPRESA").isin(["TEST1", "TEST2", "TEST3"]))
    .select(
        "EMPRESA",
        "CENTRO_COSTO_SAP",
        "cc_local",
        "cc_descripcion",
        "cc_gerencia",
        "cc_area",
        "CUENTA_GL_SAP",
        "gl_local",
        "gl_descripcion",
        "gl_tipo_gasto",
        "gl_clasificacion",
        "ANIO_FISCAL",
        "PERIODO",
        "PERIODO_LABEL",
        "MONTO_ML",
        "MONTO_USD",
        "EXECUTION_ID",
        "FECHA_PROCESO"
    )
)

total_records = df_real.count()
logger.info(f"Real records after transformation: {total_records}")

# COMMAND ----------
# DBTITLE 4, Write to curated layer (partitioned by year/month)
(
    df_real
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{curated_path}real/")
)

logger.info(f"Written to curated: {curated_path}real/")

# COMMAND ----------
# DBTITLE 5, ACID SQL write to FACT_GA_REAL
conn_str = dbutils.secrets.get(scope="kv-test", key="test-sql-connection-string")

# Collect data for bulk insert (batched for performance)
rows = df_real.collect()

if not rows:
    logger.warning("No records to load into FACT_GA_REAL. Skipping SQL write.")
    dbutils.notebook.exit("NO_DATA")

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
try:
    cursor.execute("BEGIN TRANSACTION")

    # Clear current period data before reload (idempotent execution)
    anio  = rows[0]["ANIO_FISCAL"]
    periodo = rows[0]["PERIODO"]
    cursor.execute(
        "DELETE FROM dbo.FACT_GA_REAL WHERE ANIO_FISCAL = ? AND PERIODO = ? AND EXECUTION_ID = ?",
        (anio, periodo, execution_id)
    )

    insert_sql = """
        INSERT INTO dbo.FACT_GA_REAL (
            EMPRESA, CENTRO_COSTO_SAP, CC_LOCAL, CC_DESCRIPCION, CC_GERENCIA, CC_AREA,
            CUENTA_GL_SAP, GL_LOCAL, GL_DESCRIPCION, GL_TIPO_GASTO, GL_CLASIFICACION,
            ANIO_FISCAL, PERIODO, PERIODO_LABEL, MONTO_ML, MONTO_USD,
            EXECUTION_ID, FECHA_PROCESO
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_sql, [
            (
                r["EMPRESA"], r["CENTRO_COSTO_SAP"], r["cc_local"], r["cc_descripcion"],
                r["cc_gerencia"], r["cc_area"], r["CUENTA_GL_SAP"], r["gl_local"],
                r["gl_descripcion"], r["gl_tipo_gasto"], r["gl_clasificacion"],
                r["ANIO_FISCAL"], r["PERIODO"], r["PERIODO_LABEL"],
                r["MONTO_ML"], r["MONTO_USD"], r["EXECUTION_ID"], r["FECHA_PROCESO"]
            )
            for r in batch
        ])

    conn.commit()
    logger.info(f"FACT_GA_REAL loaded: {len(rows)} records committed.")

except Exception as e:
    conn.rollback()
    logger.error(f"SQL transaction failed: {e}")
    raise e
finally:
    conn.close()

# COMMAND ----------
# DBTITLE 6, Write execution log
log_data = [{
    "execution_id":   execution_id,
    "environment":    environment,
    "notebook":       "nb_GA_02_Real",
    "domain":         "GA",
    "records_output": total_records,
    "status":         "SUCCESS",
    "timestamp":      datetime.utcnow().isoformat()
}]

df_log = spark.createDataFrame(log_data)
df_log.write.mode("append").json(f"{logs_path}execution_id={execution_id}/")

logger.info("Execution log written successfully.")
dbutils.notebook.exit("SUCCESS")
