# Databricks notebook source
# COMMAND ----------
# DBTITLE 1, Balance - Real: Transformación de Balance General y carga en FACT_BALANCE_REAL

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from datetime import datetime
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nb_Balance_01_Real")

# COMMAND ----------
# Parameters via widgets
dbutils.widgets.text("storage_account", "", "Storage Account Name")
dbutils.widgets.text("environment", "dev", "Environment (dev/stg/prd)")
dbutils.widgets.text("execution_id", "", "Execution ID")

storage_account = dbutils.widgets.get("storage_account")
environment     = dbutils.widgets.get("environment")
execution_id    = dbutils.widgets.get("execution_id")

logger.info(f"Starting nb_Balance_01_Real | env={environment} | exec_id={execution_id}")

# COMMAND ----------
# ADLS Gen2 connection via Key Vault secret scope
storage_key = dbutils.secrets.get(scope="kv-test", key="test-adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------
# Medallion paths
landing_path = f"abfss://test-landing@{storage_account}.dfs.core.windows.net/balance/"
staging_path = f"abfss://test-staging@{storage_account}.dfs.core.windows.net/balance/"
curated_path = f"abfss://test-curated@{storage_account}.dfs.core.windows.net/balance/"
logs_path    = f"abfss://test-logs@{storage_account}.dfs.core.windows.net/balance/real/"

# COMMAND ----------
# DBTITLE 2, Read SharePoint Excel from landing
df_raw = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dataAddress", "'Balance General'!A3")
    .load(f"{landing_path}real/execution_id={execution_id}/")
)

logger.info(f"Raw records from landing Excel: {df_raw.count()}")

# COMMAND ----------
# DBTITLE 3, Parse balance sheet structure
df_balance = (
    df_raw
    .withColumnRenamed("Empresa",      "EMPRESA")
    .withColumnRenamed("Cuenta",       "CUENTA_CONTABLE")
    .withColumnRenamed("Descripcion",  "DESCRIPCION_CUENTA")
    .withColumnRenamed("Clasificacion","CLASIFICACION")
    .withColumnRenamed("Grupo",        "GRUPO_BALANCE")
    .withColumnRenamed("SubGrupo",     "SUBGRUPO")
    .withColumnRenamed("Anio",         "ANIO_FISCAL")
    .withColumnRenamed("Mes",          "PERIODO")
    .withColumnRenamed("Saldo_ML",     "SALDO_ML")
    .withColumnRenamed("Saldo_USD",    "SALDO_USD")
    .withColumn("SALDO_ML",  F.col("SALDO_ML").cast(DoubleType()))
    .withColumn("SALDO_USD", F.col("SALDO_USD").cast(DoubleType()))
    .withColumn("ANIO_FISCAL", F.col("ANIO_FISCAL").cast(IntegerType()))
    .withColumn("PERIODO",     F.col("PERIODO").cast(IntegerType()))
    .withColumn("CUENTA_CONTABLE", F.trim(F.col("CUENTA_CONTABLE")))
    # Balance side: ACTIVO / PASIVO / PATRIMONIO
    .withColumn(
        "LADO_BALANCE",
        F.when(F.col("CLASIFICACION").isin(["ACTIVO CORRIENTE", "ACTIVO NO CORRIENTE"]), F.lit("ACTIVO"))
        .when(F.col("CLASIFICACION").isin(["PASIVO CORRIENTE", "PASIVO NO CORRIENTE"]), F.lit("PASIVO"))
        .when(F.col("CLASIFICACION") == "PATRIMONIO", F.lit("PATRIMONIO"))
        .otherwise(F.lit("OTRO"))
    )
    .withColumn(
        "PERIODO_LABEL",
        F.concat(F.col("ANIO_FISCAL").cast(StringType()), F.lit("-"), F.lpad(F.col("PERIODO").cast(StringType()), 2, "0"))
    )
    .withColumn("SALDO_ML",  F.round(F.col("SALDO_ML"),  2))
    .withColumn("SALDO_USD", F.round(F.col("SALDO_USD"), 2))
    .withColumn("EXECUTION_ID",  F.lit(execution_id))
    .withColumn("ENVIRONMENT",   F.lit(environment))
    .withColumn("FECHA_PROCESO", F.current_timestamp())
    .filter(F.col("CUENTA_CONTABLE").isNotNull())
    .filter(F.col("SALDO_ML").isNotNull())
)

total_records = df_balance.count()
logger.info(f"Balance records after transformation: {total_records}")

# COMMAND ----------
# DBTITLE 4, Write to staging
(
    df_balance
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{staging_path}real/execution_id={execution_id}/")
)

# COMMAND ----------
# DBTITLE 5, Write to curated
(
    df_balance
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{curated_path}real/")
)

logger.info(f"Written to curated: {curated_path}real/")

# COMMAND ----------
# DBTITLE 6, ACID SQL write to FACT_BALANCE_REAL
conn_str = dbutils.secrets.get(scope="kv-test", key="test-sql-connection-string")
rows = df_balance.collect()

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
try:
    cursor.execute("BEGIN TRANSACTION")

    cursor.execute(
        "DELETE FROM dbo.FACT_BALANCE_REAL WHERE EXECUTION_ID = ?",
        (execution_id,)
    )

    insert_sql = """
        INSERT INTO dbo.FACT_BALANCE_REAL (
            EMPRESA, CUENTA_CONTABLE, DESCRIPCION_CUENTA, CLASIFICACION,
            GRUPO_BALANCE, SUBGRUPO, LADO_BALANCE,
            ANIO_FISCAL, PERIODO, PERIODO_LABEL,
            SALDO_ML, SALDO_USD,
            EXECUTION_ID, FECHA_PROCESO
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_sql, [
            (
                r["EMPRESA"], r["CUENTA_CONTABLE"], r["DESCRIPCION_CUENTA"],
                r["CLASIFICACION"], r["GRUPO_BALANCE"], r["SUBGRUPO"], r["LADO_BALANCE"],
                r["ANIO_FISCAL"], r["PERIODO"], r["PERIODO_LABEL"],
                r["SALDO_ML"], r["SALDO_USD"],
                r["EXECUTION_ID"], r["FECHA_PROCESO"]
            )
            for r in batch
        ])

    conn.commit()
    logger.info(f"FACT_BALANCE_REAL loaded: {len(rows)} records committed.")

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
    "notebook":       "nb_Balance_01_Real",
    "domain":         "Balance",
    "records_output": total_records,
    "status":         "SUCCESS",
    "timestamp":      datetime.utcnow().isoformat()
}]

df_log = spark.createDataFrame(log_data)
df_log.write.mode("append").json(f"{logs_path}execution_id={execution_id}/")

logger.info("Execution log written successfully.")
dbutils.notebook.exit("SUCCESS")
