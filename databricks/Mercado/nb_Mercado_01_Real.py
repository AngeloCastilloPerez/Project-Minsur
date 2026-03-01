# Databricks notebook source
# COMMAND ----------
# DBTITLE 1, Mercado - Real: Transformación de datos de mercado y carga en FACT_MERCADO_REAL

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from datetime import datetime
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nb_Mercado_01_Real")

# COMMAND ----------
# Parameters via widgets
dbutils.widgets.text("storage_account", "", "Storage Account Name")
dbutils.widgets.text("environment", "dev", "Environment (dev/stg/prd)")
dbutils.widgets.text("execution_id", "", "Execution ID")

storage_account = dbutils.widgets.get("storage_account")
environment     = dbutils.widgets.get("environment")
execution_id    = dbutils.widgets.get("execution_id")

logger.info(f"Starting nb_Mercado_01_Real | env={environment} | exec_id={execution_id}")

# COMMAND ----------
# ADLS Gen2 connection via Key Vault secret scope
storage_key = dbutils.secrets.get(scope="kv-minsur", key="adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------
# Medallion paths
landing_path = f"abfss://finanzas-landing@{storage_account}.dfs.core.windows.net/mercado/"
staging_path = f"abfss://finanzas-staging@{storage_account}.dfs.core.windows.net/mercado/"
curated_path = f"abfss://finanzas-curated@{storage_account}.dfs.core.windows.net/mercado/"
logs_path    = f"abfss://finanzas-logs@{storage_account}.dfs.core.windows.net/mercado/real/"

# COMMAND ----------
# DBTITLE 2, Read SharePoint Excel from landing
df_raw = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dataAddress", "'Mercado'!A1")
    .load(f"{landing_path}real/execution_id={execution_id}/")
)

logger.info(f"Raw market records from landing Excel: {df_raw.count()}")

# COMMAND ----------
# DBTITLE 3, Transform market data
df_mercado = (
    df_raw
    .withColumnRenamed("Empresa",         "EMPRESA")
    .withColumnRenamed("Producto",        "PRODUCTO_COD")
    .withColumnRenamed("DescProducto",    "PRODUCTO_DESC")
    .withColumnRenamed("Metal",           "METAL")
    .withColumnRenamed("Unidad",          "UNIDAD_MEDIDA")
    .withColumnRenamed("Anio",            "ANIO_FISCAL")
    .withColumnRenamed("Mes",             "PERIODO")
    .withColumnRenamed("Volumen_Real",    "VOLUMEN_REAL")
    .withColumnRenamed("Precio_Real",     "PRECIO_REAL_USD")
    .withColumnRenamed("Ingreso_Real_ML", "INGRESO_REAL_ML")
    .withColumnRenamed("Ingreso_Real_USD","INGRESO_REAL_USD")
    .withColumnRenamed("Tipo_Cambio",     "TIPO_CAMBIO")
    .withColumn("VOLUMEN_REAL",      F.col("VOLUMEN_REAL").cast(DoubleType()))
    .withColumn("PRECIO_REAL_USD",   F.col("PRECIO_REAL_USD").cast(DoubleType()))
    .withColumn("INGRESO_REAL_ML",   F.col("INGRESO_REAL_ML").cast(DoubleType()))
    .withColumn("INGRESO_REAL_USD",  F.col("INGRESO_REAL_USD").cast(DoubleType()))
    .withColumn("TIPO_CAMBIO",       F.col("TIPO_CAMBIO").cast(DoubleType()))
    .withColumn("ANIO_FISCAL", F.col("ANIO_FISCAL").cast(IntegerType()))
    .withColumn("PERIODO",     F.col("PERIODO").cast(IntegerType()))
    .withColumn(
        "PERIODO_LABEL",
        F.concat(F.col("ANIO_FISCAL").cast(StringType()), F.lit("-"), F.lpad(F.col("PERIODO").cast(StringType()), 2, "0"))
    )
    # Verify ingreso calculation
    .withColumn(
        "INGRESO_CALCULADO_USD",
        F.round(F.col("VOLUMEN_REAL") * F.col("PRECIO_REAL_USD"), 2)
    )
    .withColumn("VOLUMEN_REAL",     F.round(F.col("VOLUMEN_REAL"),     4))
    .withColumn("PRECIO_REAL_USD",  F.round(F.col("PRECIO_REAL_USD"),  4))
    .withColumn("INGRESO_REAL_ML",  F.round(F.col("INGRESO_REAL_ML"),  2))
    .withColumn("INGRESO_REAL_USD", F.round(F.col("INGRESO_REAL_USD"), 2))
    .withColumn("EXECUTION_ID",  F.lit(execution_id))
    .withColumn("ENVIRONMENT",   F.lit(environment))
    .withColumn("FECHA_PROCESO", F.current_timestamp())
    .filter(F.col("PRODUCTO_COD").isNotNull())
    .filter(F.col("VOLUMEN_REAL").isNotNull())
)

total_records = df_mercado.count()
logger.info(f"Market records after transformation: {total_records}")

# COMMAND ----------
# DBTITLE 4, Write to staging
(
    df_mercado
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{staging_path}real/execution_id={execution_id}/")
)

# COMMAND ----------
# DBTITLE 5, Write to curated
(
    df_mercado
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{curated_path}real/")
)

logger.info(f"Written to curated: {curated_path}real/")

# COMMAND ----------
# DBTITLE 6, ACID SQL write to FACT_MERCADO_REAL
conn_str = dbutils.secrets.get(scope="kv-minsur", key="sql-connection-string")
rows = df_mercado.collect()

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
try:
    cursor.execute("BEGIN TRANSACTION")

    cursor.execute(
        "DELETE FROM dbo.FACT_MERCADO_REAL WHERE EXECUTION_ID = ?",
        (execution_id,)
    )

    insert_sql = """
        INSERT INTO dbo.FACT_MERCADO_REAL (
            EMPRESA, PRODUCTO_COD, PRODUCTO_DESC, METAL, UNIDAD_MEDIDA,
            ANIO_FISCAL, PERIODO, PERIODO_LABEL,
            VOLUMEN_REAL, PRECIO_REAL_USD, INGRESO_REAL_ML, INGRESO_REAL_USD,
            TIPO_CAMBIO, INGRESO_CALCULADO_USD,
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
                r["VOLUMEN_REAL"], r["PRECIO_REAL_USD"],
                r["INGRESO_REAL_ML"], r["INGRESO_REAL_USD"],
                r["TIPO_CAMBIO"], r["INGRESO_CALCULADO_USD"],
                r["EXECUTION_ID"], r["FECHA_PROCESO"]
            )
            for r in batch
        ])

    conn.commit()
    logger.info(f"FACT_MERCADO_REAL loaded: {len(rows)} records committed.")

except Exception as e:
    conn.rollback()
    logger.error(f"SQL transaction failed: {e}")
    raise e
finally:
    conn.close()

# COMMAND ----------
# DBTITLE 7, Write execution log
log_data = [{
    "execution_id":   execution_id,
    "environment":    environment,
    "notebook":       "nb_Mercado_01_Real",
    "domain":         "Mercado",
    "records_output": total_records,
    "status":         "SUCCESS",
    "timestamp":      datetime.utcnow().isoformat()
}]

df_log = spark.createDataFrame(log_data)
df_log.write.mode("append").json(f"{logs_path}execution_id={execution_id}/")

logger.info("Execution log written successfully.")
dbutils.notebook.exit("SUCCESS")
