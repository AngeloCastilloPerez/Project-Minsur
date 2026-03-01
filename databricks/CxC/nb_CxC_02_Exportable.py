# Databricks notebook source
# COMMAND ----------
# DBTITLE 1, CxC - Exportable: Dataset consolidado para consumo Power BI

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from datetime import datetime
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nb_CxC_02_Exportable")

# COMMAND ----------
# Parameters via widgets
dbutils.widgets.text("storage_account", "", "Storage Account Name")
dbutils.widgets.text("environment", "dev", "Environment (dev/stg/prd)")
dbutils.widgets.text("execution_id", "", "Execution ID")

storage_account = dbutils.widgets.get("storage_account")
environment     = dbutils.widgets.get("environment")
execution_id    = dbutils.widgets.get("execution_id")

logger.info(f"Starting nb_CxC_02_Exportable | env={environment} | exec_id={execution_id}")

# COMMAND ----------
# ADLS Gen2 connection via Key Vault secret scope
storage_key = dbutils.secrets.get(scope="kv-test", key="test-adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------
# Medallion paths
curated_path = f"abfss://test-curated@{storage_account}.dfs.core.windows.net/cxc/"
logs_path    = f"abfss://test-logs@{storage_account}.dfs.core.windows.net/cxc/exportable/"

# COMMAND ----------
# DBTITLE 2, Read curated CxC real data
df_real = spark.read.parquet(f"{curated_path}real/")
logger.info(f"Curated records: {df_real.count()}")

# COMMAND ----------
# DBTITLE 3, Build exportable: aggregate by client, bucket and period
df_exportable = (
    df_real
    .groupBy(
        "EMPRESA", "CLIENTE_SAP", "ANIO_FISCAL", "PERIODO",
        "ESTADO", "BUCKET_VENCIMIENTO", "MONEDA"
    )
    .agg(
        F.count("NRO_DOCUMENTO").alias("QTY_DOCUMENTOS"),
        F.sum("MONTO_ML").alias("TOTAL_ML"),
        F.sum("MONTO_USD").alias("TOTAL_USD"),
        F.avg("DIAS_VENCIDO").alias("PROMEDIO_DIAS_VENCIDO"),
        F.max("DIAS_VENCIDO").alias("MAX_DIAS_VENCIDO"),
        F.min("FECHA_DOCUMENTO").alias("PRIMERA_FACTURA"),
        F.max("FECHA_DOCUMENTO").alias("ULTIMA_FACTURA")
    )
    .withColumn("TOTAL_ML",  F.round(F.col("TOTAL_ML"),  2))
    .withColumn("TOTAL_USD", F.round(F.col("TOTAL_USD"), 2))
    .withColumn("PROMEDIO_DIAS_VENCIDO", F.round(F.col("PROMEDIO_DIAS_VENCIDO"), 1))
    .withColumn("EXECUTION_ID",  F.lit(execution_id))
    .withColumn("ENVIRONMENT",   F.lit(environment))
    .withColumn("FECHA_PROCESO", F.current_timestamp())
)

total_records = df_exportable.count()
logger.info(f"Exportable records: {total_records}")

# COMMAND ----------
# DBTITLE 4, Write exportable to curated
(
    df_exportable
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{curated_path}exportable/")
)

logger.info(f"Written to curated exportable: {curated_path}exportable/")

# COMMAND ----------
# DBTITLE 5, ACID SQL write to FACT_CXC_EXPORTABLE
conn_str = dbutils.secrets.get(scope="kv-test", key="test-sql-connection-string")
rows = df_exportable.collect()

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
try:
    cursor.execute("BEGIN TRANSACTION")

    cursor.execute(
        "DELETE FROM dbo.FACT_CXC_EXPORTABLE WHERE EXECUTION_ID = ?",
        (execution_id,)
    )

    insert_sql = """
        INSERT INTO dbo.FACT_CXC_EXPORTABLE (
            EMPRESA, CLIENTE_SAP, ANIO_FISCAL, PERIODO,
            ESTADO, BUCKET_VENCIMIENTO, MONEDA,
            QTY_DOCUMENTOS, TOTAL_ML, TOTAL_USD,
            PROMEDIO_DIAS_VENCIDO, MAX_DIAS_VENCIDO,
            PRIMERA_FACTURA, ULTIMA_FACTURA,
            EXECUTION_ID, FECHA_PROCESO
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_sql, [
            (
                r["EMPRESA"], r["CLIENTE_SAP"], r["ANIO_FISCAL"], r["PERIODO"],
                r["ESTADO"], r["BUCKET_VENCIMIENTO"], r["MONEDA"],
                r["QTY_DOCUMENTOS"], r["TOTAL_ML"], r["TOTAL_USD"],
                r["PROMEDIO_DIAS_VENCIDO"], r["MAX_DIAS_VENCIDO"],
                r["PRIMERA_FACTURA"], r["ULTIMA_FACTURA"],
                r["EXECUTION_ID"], r["FECHA_PROCESO"]
            )
            for r in batch
        ])

    conn.commit()
    logger.info(f"FACT_CXC_EXPORTABLE loaded: {len(rows)} records committed.")

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
    "notebook":       "nb_CxC_02_Exportable",
    "domain":         "CxC",
    "records_output": total_records,
    "status":         "SUCCESS",
    "timestamp":      datetime.utcnow().isoformat()
}]

df_log = spark.createDataFrame(log_data)
df_log.write.mode("append").json(f"{logs_path}execution_id={execution_id}/")

logger.info("Execution log written successfully.")
dbutils.notebook.exit("SUCCESS")
