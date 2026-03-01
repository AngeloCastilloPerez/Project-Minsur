# Databricks notebook source
# COMMAND ----------
# DBTITLE 1, GA - Exportable: Consolidación real + plan y carga en FACT_GA_EXPORTABLE

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from datetime import datetime
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nb_GA_04_Exportable")

# COMMAND ----------
# Parameters via widgets
dbutils.widgets.text("storage_account", "", "Storage Account Name")
dbutils.widgets.text("environment", "dev", "Environment (dev/stg/prd)")
dbutils.widgets.text("execution_id", "", "Execution ID")

storage_account = dbutils.widgets.get("storage_account")
environment     = dbutils.widgets.get("environment")
execution_id    = dbutils.widgets.get("execution_id")

logger.info(f"Starting nb_GA_04_Exportable | env={environment} | exec_id={execution_id}")

# COMMAND ----------
# ADLS Gen2 connection via Key Vault secret scope
storage_key = dbutils.secrets.get(scope="kv-test", key="test-adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------
# Medallion paths
curated_path = f"abfss://test-curated@{storage_account}.dfs.core.windows.net/ga/"
logs_path    = f"abfss://test-logs@{storage_account}.dfs.core.windows.net/ga/exportable/"

# COMMAND ----------
# DBTITLE 2, Read curated real and plan data
df_real = spark.read.parquet(f"{curated_path}real/")
df_plan = spark.read.parquet(f"{curated_path}plan/")

logger.info(f"Real records: {df_real.count()} | Plan records: {df_plan.count()}")

# COMMAND ----------
# DBTITLE 3, Join real and plan on dimension keys
join_keys = ["EMPRESA", "CENTRO_COSTO_SAP", "CUENTA_GL_SAP", "ANIO_FISCAL", "PERIODO"]

df_real_agg = (
    df_real
    .groupBy(join_keys + ["cc_local", "cc_descripcion", "cc_gerencia", "cc_area",
                          "gl_local", "gl_descripcion", "gl_tipo_gasto", "gl_clasificacion",
                          "PERIODO_LABEL"])
    .agg(
        F.sum("MONTO_ML").alias("MONTO_ML_REAL"),
        F.sum("MONTO_USD").alias("MONTO_USD_REAL")
    )
    # Rename lowercase dimension columns to uppercase for consistency
    .withColumnRenamed("cc_local",       "CC_LOCAL")
    .withColumnRenamed("cc_descripcion", "CC_DESCRIPCION")
    .withColumnRenamed("cc_gerencia",    "CC_GERENCIA")
    .withColumnRenamed("cc_area",        "CC_AREA")
    .withColumnRenamed("gl_local",       "GL_LOCAL")
    .withColumnRenamed("gl_descripcion", "GL_DESCRIPCION")
    .withColumnRenamed("gl_tipo_gasto",  "GL_TIPO_GASTO")
    .withColumnRenamed("gl_clasificacion", "GL_CLASIFICACION")
)

df_plan_agg = (
    df_plan
    .groupBy(join_keys)
    .agg(
        F.sum("MONTO_ML_PLAN").alias("MONTO_ML_PLAN"),
        F.sum("MONTO_USD_PLAN").alias("MONTO_USD_PLAN")
    )
)

df_exportable = (
    df_real_agg
    .join(df_plan_agg, on=join_keys, how="full")
    .withColumn("MONTO_ML_REAL",  F.coalesce(F.col("MONTO_ML_REAL"),  F.lit(0.0)))
    .withColumn("MONTO_USD_REAL", F.coalesce(F.col("MONTO_USD_REAL"), F.lit(0.0)))
    .withColumn("MONTO_ML_PLAN",  F.coalesce(F.col("MONTO_ML_PLAN"),  F.lit(0.0)))
    .withColumn("MONTO_USD_PLAN", F.coalesce(F.col("MONTO_USD_PLAN"), F.lit(0.0)))
    # Variance: real vs plan
    .withColumn("VAR_ML",  F.round(F.col("MONTO_ML_REAL")  - F.col("MONTO_ML_PLAN"),  2))
    .withColumn("VAR_USD", F.round(F.col("MONTO_USD_REAL") - F.col("MONTO_USD_PLAN"), 2))
    # Percentage variance (safe division)
    .withColumn(
        "VAR_PCT_ML",
        F.when(F.col("MONTO_ML_PLAN") != 0,
               F.round((F.col("MONTO_ML_REAL") - F.col("MONTO_ML_PLAN")) / F.col("MONTO_ML_PLAN") * 100, 2)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
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
# DBTITLE 5, ACID SQL write to FACT_GA_EXPORTABLE
conn_str = dbutils.secrets.get(scope="kv-test", key="test-sql-connection-string")
rows = df_exportable.collect()

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
try:
    cursor.execute("BEGIN TRANSACTION")

    cursor.execute(
        "DELETE FROM dbo.FACT_GA_EXPORTABLE WHERE EXECUTION_ID = ?",
        (execution_id,)
    )

    insert_sql = """
        INSERT INTO dbo.FACT_GA_EXPORTABLE (
            EMPRESA, CENTRO_COSTO_SAP, CC_LOCAL, CC_DESCRIPCION, CC_GERENCIA, CC_AREA,
            CUENTA_GL_SAP, GL_LOCAL, GL_DESCRIPCION, GL_TIPO_GASTO, GL_CLASIFICACION,
            ANIO_FISCAL, PERIODO, PERIODO_LABEL,
            MONTO_ML_REAL, MONTO_USD_REAL, MONTO_ML_PLAN, MONTO_USD_PLAN,
            VAR_ML, VAR_USD, VAR_PCT_ML,
            EXECUTION_ID, FECHA_PROCESO
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_sql, [
            (
                r["EMPRESA"], r["CENTRO_COSTO_SAP"], r["CC_LOCAL"], r["CC_DESCRIPCION"],
                r["CC_GERENCIA"], r["CC_AREA"], r["CUENTA_GL_SAP"], r["GL_LOCAL"],
                r["GL_DESCRIPCION"], r["GL_TIPO_GASTO"], r["GL_CLASIFICACION"],
                r["ANIO_FISCAL"], r["PERIODO"], r["PERIODO_LABEL"],
                r["MONTO_ML_REAL"], r["MONTO_USD_REAL"],
                r["MONTO_ML_PLAN"], r["MONTO_USD_PLAN"],
                r["VAR_ML"], r["VAR_USD"], r["VAR_PCT_ML"],
                r["EXECUTION_ID"], r["FECHA_PROCESO"]
            )
            for r in batch
        ])

    conn.commit()
    logger.info(f"FACT_GA_EXPORTABLE loaded: {len(rows)} records committed.")

except Exception as e:
    conn.rollback()
    logger.error(f"SQL transaction failed: {e}")
    raise e
finally:
    conn.close()

# COMMAND ----------
# DBTITLE 6, Final execution summary log
log_data = [{
    "execution_id":    execution_id,
    "environment":     environment,
    "notebook":        "nb_GA_04_Exportable",
    "domain":          "GA",
    "records_real":    df_real.count(),
    "records_plan":    df_plan.count(),
    "records_output":  total_records,
    "status":          "SUCCESS",
    "timestamp":       datetime.utcnow().isoformat()
}]

df_log = spark.createDataFrame(log_data)
df_log.write.mode("append").json(f"{logs_path}execution_id={execution_id}/")

logger.info("Execution log written successfully.")
dbutils.notebook.exit("SUCCESS")
