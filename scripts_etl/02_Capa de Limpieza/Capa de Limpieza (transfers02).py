# Databricks notebook source
#Celda 1
#----------------------

from pyspark.sql import functions as F

# Widgets
dbutils.widgets.text("RAW_PATH",   "abfss://datos@.../raw/")
dbutils.widgets.text("CLEAN_PATH", "abfss://datos@.../clean/")

RAW_PATH   = dbutils.widgets.get("RAW_PATH")
CLEAN_PATH = dbutils.widgets.get("CLEAN_PATH")

# Lee solo transfers desde RAW

df = spark.read.format("delta").load(f"{RAW_PATH}transfers")
print("transfers cargado ✅")

# Comprobacion de esquema actual en CLEAN para comparar
try:
    print("→ Esquema actual en CLEAN/transfers:")
    spark.read.format("delta").load(f"{CLEAN_PATH}transfers").printSchema()
except Exception:
    print("→ CLEAN/transfers aún no existe")

# Limpieza
df_clean = (
    df.filter(F.col("from_stop_id").isNotNull() & F.col("to_stop_id").isNotNull())
      .dropDuplicates(["from_stop_id","to_stop_id"])
      .withColumn("transfer_type", F.col("transfer_type").cast("int"))
      .withColumn("min_transfer_time", F.col("min_transfer_time").cast("int"))
    # .filter(F.col("transfer_type").isin(0,1,2,3))  # opcional
)

print("→ Esquema que vas a escribir:")
df_clean.printSchema()

# Escritura en zona CLEAN forzando el esquema
df_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{CLEAN_PATH}transfers")

print("transfers → CLEAN ✅")
dbutils.notebook.exit("OK")