# Databricks notebook source
#Celda 1
#----------------------

from pyspark.sql import functions as F

# Widgets
dbutils.widgets.text("RAW_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/")
dbutils.widgets.text("CLEAN_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/")
RAW_PATH=dbutils.widgets.get("RAW_PATH"); CLEAN_PATH=dbutils.widgets.get("CLEAN_PATH")

# Lectura desde RAW

df = spark.read.format("delta").load(f"{RAW_PATH}translations")
print("translations cargado ✅")

# Limpieza
df_clean = df.dropDuplicates()  # ajusta reglas según columnas reales

# Guardado en zona CLEAN 

df_clean.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}translations")
print("translations → CLEAN ✅")
dbutils.notebook.exit("OK")