# Databricks notebook source
# COMMAND ----------
# DBTITLE 1, GA - Homologaciones: Limpieza y mapeo de centros de costo y cuentas GL

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nb_GA_01_Homologaciones")

# COMMAND ----------
# Parameters via widgets
dbutils.widgets.text("storage_account", "", "Storage Account Name")
dbutils.widgets.text("environment", "dev", "Environment (dev/stg/prd)")
dbutils.widgets.text("execution_id", "", "Execution ID")

storage_account = dbutils.widgets.get("storage_account")
environment     = dbutils.widgets.get("environment")
execution_id    = dbutils.widgets.get("execution_id")

logger.info(f"Starting nb_GA_01_Homologaciones | env={environment} | exec_id={execution_id}")

# COMMAND ----------
# ADLS Gen2 connection via Key Vault secret scope
storage_key = dbutils.secrets.get(scope="kv-minsur", key="adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------
# Medallion paths for G&A
landing_path = f"abfss://finanzas-landing@{storage_account}.dfs.core.windows.net/ga/"
staging_path = f"abfss://finanzas-staging@{storage_account}.dfs.core.windows.net/ga/"
logs_path    = f"abfss://finanzas-logs@{storage_account}.dfs.core.windows.net/ga/homologaciones/"

# COMMAND ----------
# DBTITLE 2, Read raw GA data from landing (JSON from SAP SOAP Azure Function)
df_raw = (
    spark.read
    .option("multiLine", "true")
    .json(f"{landing_path}real/execution_id={execution_id}/")
    # Azure Function wraps records under a "data" array; explode it
    .select(F.explode("data").alias("row"))
    .select("row.*")
)

logger.info(f"Raw records read from landing: {df_raw.count()}")

# COMMAND ----------
# DBTITLE 3, Read homologation mapping tables from ADLS (reference data)
df_cc_map = (
    spark.read
    .option("header", "true")
    .csv(f"{landing_path}reference/homologacion_centros_costo.csv")
    .select(
        F.col("CENTRO_COSTO_SAP"),
        F.col("CENTRO_COSTO_LOCAL").alias("cc_local"),
        F.col("DESCRIPCION").alias("cc_descripcion"),
        F.col("GERENCIA").alias("cc_gerencia"),
        F.col("AREA").alias("cc_area")
    )
)

df_gl_map = (
    spark.read
    .option("header", "true")
    .csv(f"{landing_path}reference/homologacion_cuentas_gl.csv")
    .select(
        F.col("CUENTA_GL_SAP"),
        F.col("CUENTA_GL_LOCAL").alias("gl_local"),
        F.col("DESCRIPCION_GL").alias("gl_descripcion"),
        F.col("TIPO_GASTO").alias("gl_tipo_gasto"),
        F.col("CLASIFICACION").alias("gl_clasificacion")
    )
)

logger.info(f"CC mapping records: {df_cc_map.count()} | GL mapping records: {df_gl_map.count()}")

# COMMAND ----------
# DBTITLE 4, Standardize column names and data types
df_std = (
    df_raw
    .withColumnRenamed("BUKRS", "EMPRESA")
    .withColumnRenamed("KOSTL", "CENTRO_COSTO_SAP")
    .withColumnRenamed("SAKNR", "CUENTA_GL_SAP")
    .withColumnRenamed("GJAHR", "ANIO_FISCAL")
    .withColumnRenamed("MONAT", "PERIODO")
    .withColumnRenamed("WKGBTR", "MONTO_ML")
    .withColumnRenamed("WTGBTR", "MONTO_USD")
    .withColumn("MONTO_ML",  F.col("MONTO_ML").cast(DoubleType()))
    .withColumn("MONTO_USD", F.col("MONTO_USD").cast(DoubleType()))
    .withColumn("ANIO_FISCAL", F.col("ANIO_FISCAL").cast(IntegerType()))
    .withColumn("PERIODO",     F.col("PERIODO").cast(IntegerType()))
    .withColumn("CENTRO_COSTO_SAP", F.trim(F.col("CENTRO_COSTO_SAP")))
    .withColumn("CUENTA_GL_SAP",    F.trim(F.col("CUENTA_GL_SAP")))
    .filter(F.col("MONTO_ML").isNotNull())
)

# COMMAND ----------
# DBTITLE 5, Apply cost center homologation
df_with_cc = df_std.join(df_cc_map, on="CENTRO_COSTO_SAP", how="left")

# Flag records without mapping for audit
unmapped_cc = df_with_cc.filter(F.col("cc_local").isNull()).count()
if unmapped_cc > 0:
    logger.warning(f"Records without CC homologation: {unmapped_cc}")

# COMMAND ----------
# DBTITLE 6, Apply GL account homologation
df_homologated = (
    df_with_cc
    .join(df_gl_map, on="CUENTA_GL_SAP", how="left")
    .withColumn("EXECUTION_ID", F.lit(execution_id))
    .withColumn("ENVIRONMENT",  F.lit(environment))
    .withColumn("FECHA_PROCESO", F.current_timestamp())
    .withColumn("FLAG_HOMOLOGADO_CC", F.when(F.col("cc_local").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("FLAG_HOMOLOGADO_GL", F.when(F.col("gl_local").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
)

unmapped_gl = df_homologated.filter(F.col("gl_local").isNull()).count()
if unmapped_gl > 0:
    logger.warning(f"Records without GL homologation: {unmapped_gl}")

total_records = df_homologated.count()
logger.info(f"Homologated records: {total_records}")

# COMMAND ----------
# DBTITLE 7, Write homologated data to staging as Parquet
(
    df_homologated
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{staging_path}homologaciones/execution_id={execution_id}/")
)

logger.info(f"Written to staging: {staging_path}homologaciones/execution_id={execution_id}/")

# COMMAND ----------
# DBTITLE 8, Write execution log
log_data = [{
    "execution_id":    execution_id,
    "environment":     environment,
    "notebook":        "nb_GA_01_Homologaciones",
    "domain":          "GA",
    "records_input":   df_raw.count(),
    "records_output":  total_records,
    "unmapped_cc":     unmapped_cc,
    "unmapped_gl":     unmapped_gl,
    "status":          "SUCCESS",
    "timestamp":       datetime.utcnow().isoformat()
}]

df_log = spark.createDataFrame(log_data)
(
    df_log
    .write
    .mode("append")
    .json(f"{logs_path}execution_id={execution_id}/")
)

logger.info("Execution log written successfully.")
dbutils.notebook.exit("SUCCESS")
