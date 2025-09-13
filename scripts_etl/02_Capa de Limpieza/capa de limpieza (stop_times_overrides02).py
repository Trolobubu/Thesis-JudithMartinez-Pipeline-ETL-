# Databricks notebook source
#Celda 1
#-----------------------------------------------

from pyspark.sql import functions as F

# Widgets
dbutils.widgets.text("RAW_PATH", "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/")
dbutils.widgets.text("CLEAN_PATH", "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/")

RAW_PATH   = dbutils.widgets.get("RAW_PATH")
CLEAN_PATH = dbutils.widgets.get("CLEAN_PATH")

# Leer solo stop_time_overrides desde RAW
df = spark.read.format("delta").load(f"{RAW_PATH}stop_time_overrides")
print("stop_time_overrides cargado ✅")

# Limpieza de duplicados
df_clean = (
    df.dropDuplicates()
)

# Guardado en zona CLEAN 
df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .save(f"{CLEAN_PATH}stop_time_overrides")

print("stop_time_overrides → CLEAN ✅")
dbutils.notebook.exit("OK")