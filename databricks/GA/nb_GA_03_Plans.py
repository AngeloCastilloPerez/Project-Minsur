# Databricks notebook source
# COMMAND ----------
# DBTITLE 1, GA - Plans: Transformación de datos de plan/presupuesto y carga en FACT_GA_PLAN

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from datetime import datetime
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nb_GA_03_Plans")

# COMMAND ----------
# Parameters via widgets
dbutils.widgets.text("storage_account", "", "Storage Account Name")
dbutils.widgets.text("environment", "dev", "Environment (dev/stg/prd)")
dbutils.widgets.text("execution_id", "", "Execution ID")

storage_account = dbutils.widgets.get("storage_account")
environment     = dbutils.widgets.get("environment")
execution_id    = dbutils.widgets.get("execution_id")

logger.info(f"Starting nb_GA_03_Plans | env={environment} | exec_id={execution_id}")

# COMMAND ----------
# ADLS Gen2 connection via Key Vault secret scope
storage_key = dbutils.secrets.get(scope="kv-test", key="test-adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------
# Medallion paths
landing_path = f"abfss://test-landing@{storage_account}.dfs.core.windows.net/ga/"
staging_path = f"abfss://test-staging@{storage_account}.dfs.core.windows.net/ga/"
curated_path = f"abfss://test-curated@{storage_account}.dfs.core.windows.net/ga/"
logs_path    = f"abfss://test-logs@{storage_account}.dfs.core.windows.net/ga/plans/"

# COMMAND ----------
# DBTITLE 2, Read plan data from landing (Excel uploaded from SharePoint via ADF)
df_plan_raw = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dataAddress", "'Plan GA'!A1")
    .load(f"{landing_path}plan/execution_id={execution_id}/")
)

logger.info(f"Raw plan records from landing: {df_plan_raw.count()}")

# COMMAND ----------
# DBTITLE 3, Read homologation mappings from staging reference
df_cc_map = spark.read.parquet(f"{staging_path}homologaciones/execution_id={execution_id}/").select(
    "CENTRO_COSTO_SAP", "cc_local", "cc_descripcion", "cc_gerencia", "cc_area"
).distinct()

df_gl_map = spark.read.parquet(f"{staging_path}homologaciones/execution_id={execution_id}/").select(
    "CUENTA_GL_SAP", "gl_local", "gl_descripcion", "gl_tipo_gasto", "gl_clasificacion"
).distinct()

# COMMAND ----------
# DBTITLE 4, Standardize and transform plan data
df_plan_std = (
    df_plan_raw
    .withColumnRenamed("Empresa",        "EMPRESA")
    .withColumnRenamed("Centro_Costo",   "CENTRO_COSTO_SAP")
    .withColumnRenamed("Cuenta_GL",      "CUENTA_GL_SAP")
    .withColumnRenamed("Anio",           "ANIO_FISCAL")
    .withColumnRenamed("Periodo",        "PERIODO")
    .withColumnRenamed("Monto_ML_Plan",  "MONTO_ML_PLAN")
    .withColumnRenamed("Monto_USD_Plan", "MONTO_USD_PLAN")
    .withColumn("MONTO_ML_PLAN",  F.col("MONTO_ML_PLAN").cast(DoubleType()))
    .withColumn("MONTO_USD_PLAN", F.col("MONTO_USD_PLAN").cast(DoubleType()))
    .withColumn("ANIO_FISCAL", F.col("ANIO_FISCAL").cast(IntegerType()))
    .withColumn("PERIODO",     F.col("PERIODO").cast(IntegerType()))
    .withColumn("CENTRO_COSTO_SAP", F.trim(F.col("CENTRO_COSTO_SAP")))
    .withColumn("CUENTA_GL_SAP",    F.trim(F.col("CUENTA_GL_SAP")))
    .filter(F.col("MONTO_ML_PLAN").isNotNull())
)

# Apply homologation mappings
df_plan = (
    df_plan_std
    .join(df_cc_map, on="CENTRO_COSTO_SAP", how="left")
    .join(df_gl_map, on="CUENTA_GL_SAP", how="left")
    .withColumn(
        "PERIODO_LABEL",
        F.concat(F.col("ANIO_FISCAL").cast(StringType()), F.lit("-"), F.lpad(F.col("PERIODO").cast(StringType()), 2, "0"))
    )
    .withColumn("MONTO_ML_PLAN",  F.round(F.col("MONTO_ML_PLAN"),  2))
    .withColumn("MONTO_USD_PLAN", F.round(F.col("MONTO_USD_PLAN"), 2))
    .withColumn("EXECUTION_ID", F.lit(execution_id))
    .withColumn("ENVIRONMENT",  F.lit(environment))
    .withColumn("FECHA_PROCESO", F.current_timestamp())
    .select(
        "EMPRESA", "CENTRO_COSTO_SAP", "cc_local", "cc_descripcion", "cc_gerencia", "cc_area",
        "CUENTA_GL_SAP", "gl_local", "gl_descripcion", "gl_tipo_gasto", "gl_clasificacion",
        "ANIO_FISCAL", "PERIODO", "PERIODO_LABEL",
        "MONTO_ML_PLAN", "MONTO_USD_PLAN",
        "EXECUTION_ID", "FECHA_PROCESO"
    )
)

total_records = df_plan.count()
logger.info(f"Plan records after transformation: {total_records}")

# COMMAND ----------
# DBTITLE 5, Write to curated layer
(
    df_plan
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{curated_path}plan/")
)

logger.info(f"Written to curated: {curated_path}plan/")

# COMMAND ----------
# DBTITLE 6, ACID SQL write to FACT_GA_PLAN
conn_str = dbutils.secrets.get(scope="kv-test", key="test-sql-connection-string")
rows = df_plan.collect()

if not rows:
    logger.warning("No records to load into FACT_GA_PLAN. Skipping SQL write.")
    dbutils.notebook.exit("NO_DATA")

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
try:
    cursor.execute("BEGIN TRANSACTION")

    anio    = rows[0]["ANIO_FISCAL"]
    periodo = rows[0]["PERIODO"]
    cursor.execute(
        "DELETE FROM dbo.FACT_GA_PLAN WHERE ANIO_FISCAL = ? AND PERIODO = ? AND EXECUTION_ID = ?",
        (anio, periodo, execution_id)
    )

    insert_sql = """
        INSERT INTO dbo.FACT_GA_PLAN (
            EMPRESA, CENTRO_COSTO_SAP, CC_LOCAL, CC_DESCRIPCION, CC_GERENCIA, CC_AREA,
            CUENTA_GL_SAP, GL_LOCAL, GL_DESCRIPCION, GL_TIPO_GASTO, GL_CLASIFICACION,
            ANIO_FISCAL, PERIODO, PERIODO_LABEL, MONTO_ML_PLAN, MONTO_USD_PLAN,
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
                r["MONTO_ML_PLAN"], r["MONTO_USD_PLAN"],
                r["EXECUTION_ID"], r["FECHA_PROCESO"]
            )
            for r in batch
        ])

    conn.commit()
    logger.info(f"FACT_GA_PLAN loaded: {len(rows)} records committed.")

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
    "notebook":       "nb_GA_03_Plans",
    "domain":         "GA",
    "records_output": total_records,
    "status":         "SUCCESS",
    "timestamp":      datetime.utcnow().isoformat()
}]

df_log = spark.createDataFrame(log_data)
df_log.write.mode("append").json(f"{logs_path}execution_id={execution_id}/")

logger.info("Execution log written successfully.")
dbutils.notebook.exit("SUCCESS")
