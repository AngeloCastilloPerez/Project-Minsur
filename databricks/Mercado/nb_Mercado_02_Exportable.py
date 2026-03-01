# Databricks notebook source
# COMMAND ----------
# DBTITLE 1, Mercado - Exportable: Dataset consolidado mercado para Power BI

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from datetime import datetime
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nb_Mercado_02_Exportable")

# COMMAND ----------
# Parameters via widgets
dbutils.widgets.text("storage_account", "", "Storage Account Name")
dbutils.widgets.text("environment", "dev", "Environment (dev/stg/prd)")
dbutils.widgets.text("execution_id", "", "Execution ID")

storage_account = dbutils.widgets.get("storage_account")
environment     = dbutils.widgets.get("environment")
execution_id    = dbutils.widgets.get("execution_id")

logger.info(f"Starting nb_Mercado_02_Exportable | env={environment} | exec_id={execution_id}")

# COMMAND ----------
# ADLS Gen2 connection via Key Vault secret scope
storage_key = dbutils.secrets.get(scope="kv-minsur", key="adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------
# Medallion paths
curated_path = f"abfss://finanzas-curated@{storage_account}.dfs.core.windows.net/mercado/"
logs_path    = f"abfss://finanzas-logs@{storage_account}.dfs.core.windows.net/mercado/exportable/"

# COMMAND ----------
# DBTITLE 2, Read curated market real data
df_real = spark.read.parquet(f"{curated_path}real/")
logger.info(f"Curated records: {df_real.count()}")

# COMMAND ----------
# DBTITLE 3, Build exportable: aggregate by product, metal and period
df_exportable = (
    df_real
    .groupBy(
        "EMPRESA", "PRODUCTO_COD", "PRODUCTO_DESC", "METAL",
        "UNIDAD_MEDIDA", "ANIO_FISCAL", "PERIODO", "PERIODO_LABEL"
    )
    .agg(
        F.sum("VOLUMEN_REAL").alias("TOTAL_VOLUMEN"),
        F.avg("PRECIO_REAL_USD").alias("PRECIO_PROMEDIO_USD"),
        F.sum("INGRESO_REAL_ML").alias("TOTAL_INGRESO_ML"),
        F.sum("INGRESO_REAL_USD").alias("TOTAL_INGRESO_USD"),
        F.avg("TIPO_CAMBIO").alias("TIPO_CAMBIO_PROMEDIO")
    )
    .withColumn("TOTAL_VOLUMEN",        F.round(F.col("TOTAL_VOLUMEN"),        4))
    .withColumn("PRECIO_PROMEDIO_USD",  F.round(F.col("PRECIO_PROMEDIO_USD"),  4))
    .withColumn("TOTAL_INGRESO_ML",     F.round(F.col("TOTAL_INGRESO_ML"),     2))
    .withColumn("TOTAL_INGRESO_USD",    F.round(F.col("TOTAL_INGRESO_USD"),    2))
    .withColumn("TIPO_CAMBIO_PROMEDIO", F.round(F.col("TIPO_CAMBIO_PROMEDIO"), 4))
    # Revenue per unit check
    .withColumn(
        "INGRESO_POR_UNIDAD_USD",
        F.when(F.col("TOTAL_VOLUMEN") != 0,
               F.round(F.col("TOTAL_INGRESO_USD") / F.col("TOTAL_VOLUMEN"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    .withColumn("EXECUTION_ID",  F.lit(execution_id))
    .withColumn("ENVIRONMENT",   F.lit(environment))
    .withColumn("FECHA_PROCESO", F.current_timestamp())
)

total_records = df_exportable.count()
logger.info(f"Exportable records: {total_records}")

# COMMAND ----------
# DBTITLE 4, Write to curated exportable
(
    df_exportable
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{curated_path}exportable/")
)

logger.info(f"Written to curated exportable: {curated_path}exportable/")

# COMMAND ----------
# DBTITLE 5, ACID SQL write to FACT_MERCADO_EXPORTABLE
conn_str = dbutils.secrets.get(scope="kv-minsur", key="sql-connection-string")
rows = df_exportable.collect()

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
try:
    cursor.execute("BEGIN TRANSACTION")

    cursor.execute(
        "DELETE FROM dbo.FACT_MERCADO_EXPORTABLE WHERE EXECUTION_ID = ?",
        (execution_id,)
    )

    insert_sql = """
        INSERT INTO dbo.FACT_MERCADO_EXPORTABLE (
            EMPRESA, PRODUCTO_COD, PRODUCTO_DESC, METAL, UNIDAD_MEDIDA,
            ANIO_FISCAL, PERIODO, PERIODO_LABEL,
            TOTAL_VOLUMEN, PRECIO_PROMEDIO_USD, TOTAL_INGRESO_ML, TOTAL_INGRESO_USD,
            TIPO_CAMBIO_PROMEDIO, INGRESO_POR_UNIDAD_USD,
            EXECUTION_ID, FECHA_PROCESO
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_sql, [
            (
                r["EMPRESA"], r["PRODUCTO_COD"], r["PRODUCTO_DESC"],
                r["METAL"], r["UNIDAD_MEDIDA"],
                r["ANIO_FISCAL"], r["PERIODO"], r["PERIODO_LABEL"],
                r["TOTAL_VOLUMEN"], r["PRECIO_PROMEDIO_USD"],
                r["TOTAL_INGRESO_ML"], r["TOTAL_INGRESO_USD"],
                r["TIPO_CAMBIO_PROMEDIO"], r["INGRESO_POR_UNIDAD_USD"],
                r["EXECUTION_ID"], r["FECHA_PROCESO"]
            )
            for r in batch
        ])

    conn.commit()
    logger.info(f"FACT_MERCADO_EXPORTABLE loaded: {len(rows)} records committed.")

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
    "notebook":       "nb_Mercado_02_Exportable",
    "domain":         "Mercado",
    "records_output": total_records,
    "status":         "SUCCESS",
    "timestamp":      datetime.utcnow().isoformat()
}]

df_log = spark.createDataFrame(log_data)
df_log.write.mode("append").json(f"{logs_path}execution_id={execution_id}/")

logger.info("Execution log written successfully.")
dbutils.notebook.exit("SUCCESS")
