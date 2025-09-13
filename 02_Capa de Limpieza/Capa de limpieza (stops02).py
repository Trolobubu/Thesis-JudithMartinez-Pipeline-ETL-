# Databricks notebook source
#Celda 1
#----------------------

from pyspark.sql import functions as F
# Widgets
dbutils.widgets.text("RAW_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/")
dbutils.widgets.text("CLEAN_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/")
RAW_PATH=dbutils.widgets.get("RAW_PATH"); CLEAN_PATH=dbutils.widgets.get("CLEAN_PATH")

# Lectura desde RAW

df = spark.read.format("delta").load(f"{RAW_PATH}stops")
print("stops cargado ✅")

# Limpieza

df_clean = (
    df.filter(F.col("stop_id").isNotNull())
      .dropDuplicates(["stop_id"])
      .withColumn("stop_name", F.upper(F.col("stop_name")))
      .withColumn("stop_lat", F.col("stop_lat").cast("double"))
      .withColumn("stop_lon", F.col("stop_lon").cast("double"))
      .filter(F.col("stop_lat").between(-90,90) & F.col("stop_lon").between(-180,180)) # opcional
      # .fillna({"stop_code":"UNKNOWN","zone_id":"NA","stop_url":""})  # opcional
)
# Guardado en zona CLEAN 
df_clean.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}stops")
print("stops → CLEAN ✅")
dbutils.notebook.exit("OK")