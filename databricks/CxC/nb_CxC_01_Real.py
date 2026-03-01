# Databricks notebook source
# COMMAND ----------
# DBTITLE 1, CxC - Real: Transformación de cuentas por cobrar y carga en FACT_CXC_REAL

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
from datetime import datetime
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nb_CxC_01_Real")

# COMMAND ----------
# Parameters via widgets
dbutils.widgets.text("storage_account", "", "Storage Account Name")
dbutils.widgets.text("environment", "dev", "Environment (dev/stg/prd)")
dbutils.widgets.text("execution_id", "", "Execution ID")

storage_account = dbutils.widgets.get("storage_account")
environment     = dbutils.widgets.get("environment")
execution_id    = dbutils.widgets.get("execution_id")

logger.info(f"Starting nb_CxC_01_Real | env={environment} | exec_id={execution_id}")

# COMMAND ----------
# ADLS Gen2 connection via Key Vault secret scope
storage_key = dbutils.secrets.get(scope="kv-minsur", key="adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# COMMAND ----------
# Medallion paths
landing_path = f"abfss://finanzas-landing@{storage_account}.dfs.core.windows.net/cxc/"
staging_path = f"abfss://finanzas-staging@{storage_account}.dfs.core.windows.net/cxc/"
curated_path = f"abfss://finanzas-curated@{storage_account}.dfs.core.windows.net/cxc/"
logs_path    = f"abfss://finanzas-logs@{storage_account}.dfs.core.windows.net/cxc/real/"

# COMMAND ----------
# DBTITLE 2, Read landing data (Parquet from ADF CopyActivity – Azure SQL replica)
df_landing = (
    spark.read
    .parquet(f"{landing_path}real/execution_id={execution_id}/")
)

logger.info(f"Landing records: {df_landing.count()}")

# COMMAND ----------
# DBTITLE 3, Standardize and clean accounts receivable data
df_std = (
    df_landing
    .withColumnRenamed("BUKRS",    "EMPRESA")
    .withColumnRenamed("KUNNR",    "CLIENTE_SAP")
    .withColumnRenamed("BELNR",    "NRO_DOCUMENTO")
    .withColumnRenamed("BLDAT",    "FECHA_DOCUMENTO")
    .withColumnRenamed("FAEDT",    "FECHA_VENCIMIENTO")
    .withColumnRenamed("DMBTR",    "MONTO_ML")
    .withColumnRenamed("WRBTR",    "MONTO_USD")
    .withColumnRenamed("WAERS",    "MONEDA")
    .withColumnRenamed("AUGBL",    "DOC_COMPENSACION")
    .withColumnRenamed("AUGDT",    "FECHA_COMPENSACION")
    .withColumnRenamed("ZFBDT",    "FECHA_BASE_PAGO")
    .withColumnRenamed("KOART",    "TIPO_CUENTA")
    .withColumn("FECHA_DOCUMENTO",    F.to_date(F.col("FECHA_DOCUMENTO"),    "yyyyMMdd"))
    .withColumn("FECHA_VENCIMIENTO",  F.to_date(F.col("FECHA_VENCIMIENTO"),  "yyyyMMdd"))
    .withColumn("FECHA_COMPENSACION", F.to_date(F.col("FECHA_COMPENSACION"), "yyyyMMdd"))
    .withColumn("FECHA_BASE_PAGO",    F.to_date(F.col("FECHA_BASE_PAGO"),    "yyyyMMdd"))
    .withColumn("MONTO_ML",  F.col("MONTO_ML").cast(DoubleType()))
    .withColumn("MONTO_USD", F.col("MONTO_USD").cast(DoubleType()))
    .withColumn("CLIENTE_SAP", F.lpad(F.trim(F.col("CLIENTE_SAP")), 10, "0"))
    # Calculate days overdue
    .withColumn(
        "DIAS_VENCIDO",
        F.when(
            F.col("FECHA_COMPENSACION").isNull(),
            F.datediff(F.current_date(), F.col("FECHA_VENCIMIENTO"))
        ).otherwise(F.lit(0))
    )
    # Status: open vs cleared
    .withColumn(
        "ESTADO",
        F.when(F.col("DOC_COMPENSACION").isNull(), F.lit("ABIERTO")).otherwise(F.lit("COMPENSADO"))
    )
    # Aging bucket
    .withColumn(
        "BUCKET_VENCIMIENTO",
        F.when(F.col("ESTADO") == "COMPENSADO", F.lit("COMPENSADO"))
        .when(F.col("DIAS_VENCIDO") <= 0,   F.lit("VIGENTE"))
        .when(F.col("DIAS_VENCIDO") <= 30,  F.lit("1-30 DIAS"))
        .when(F.col("DIAS_VENCIDO") <= 60,  F.lit("31-60 DIAS"))
        .when(F.col("DIAS_VENCIDO") <= 90,  F.lit("61-90 DIAS"))
        .otherwise(F.lit("+90 DIAS"))
    )
    .withColumn("ANIO_FISCAL",  F.year(F.col("FECHA_DOCUMENTO")))
    .withColumn("PERIODO",      F.month(F.col("FECHA_DOCUMENTO")))
    .withColumn("EXECUTION_ID", F.lit(execution_id))
    .withColumn("FECHA_PROCESO", F.current_timestamp())
    .filter(F.col("TIPO_CUENTA") == "D")  # Debtors only
)

# COMMAND ----------
# DBTITLE 4, Write to staging
(
    df_std
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{staging_path}real/execution_id={execution_id}/")
)

logger.info(f"Written to staging: {staging_path}real/")

# COMMAND ----------
# DBTITLE 5, Write to curated
(
    df_std
    .write
    .mode("overwrite")
    .partitionBy("ANIO_FISCAL", "PERIODO")
    .parquet(f"{curated_path}real/")
)

total_records = df_std.count()
logger.info(f"Written to curated: {curated_path}real/ | Records: {total_records}")

# COMMAND ----------
# DBTITLE 6, ACID SQL write to FACT_CXC_REAL
conn_str = dbutils.secrets.get(scope="kv-minsur", key="sql-connection-string")
rows = df_std.collect()

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
try:
    cursor.execute("BEGIN TRANSACTION")

    cursor.execute(
        "DELETE FROM dbo.FACT_CXC_REAL WHERE EXECUTION_ID = ?",
        (execution_id,)
    )

    insert_sql = """
        INSERT INTO dbo.FACT_CXC_REAL (
            EMPRESA, CLIENTE_SAP, NRO_DOCUMENTO, FECHA_DOCUMENTO, FECHA_VENCIMIENTO,
            MONTO_ML, MONTO_USD, MONEDA, DOC_COMPENSACION, FECHA_COMPENSACION,
            FECHA_BASE_PAGO, TIPO_CUENTA, DIAS_VENCIDO, ESTADO, BUCKET_VENCIMIENTO,
            ANIO_FISCAL, PERIODO, EXECUTION_ID, FECHA_PROCESO
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_sql, [
            (
                r["EMPRESA"], r["CLIENTE_SAP"], r["NRO_DOCUMENTO"],
                r["FECHA_DOCUMENTO"], r["FECHA_VENCIMIENTO"],
                r["MONTO_ML"], r["MONTO_USD"], r["MONEDA"],
                r["DOC_COMPENSACION"], r["FECHA_COMPENSACION"],
                r["FECHA_BASE_PAGO"], r["TIPO_CUENTA"],
                r["DIAS_VENCIDO"], r["ESTADO"], r["BUCKET_VENCIMIENTO"],
                r["ANIO_FISCAL"], r["PERIODO"],
                r["EXECUTION_ID"], r["FECHA_PROCESO"]
            )
            for r in batch
        ])

    conn.commit()
    logger.info(f"FACT_CXC_REAL loaded: {len(rows)} records committed.")

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
    "notebook":       "nb_CxC_01_Real",
    "domain":         "CxC",
    "records_output": total_records,
    "status":         "SUCCESS",
    "timestamp":      datetime.utcnow().isoformat()
}]

df_log = spark.createDataFrame(log_data)
df_log.write.mode("append").json(f"{logs_path}execution_id={execution_id}/")

logger.info("Execution log written successfully.")
dbutils.notebook.exit("SUCCESS")
