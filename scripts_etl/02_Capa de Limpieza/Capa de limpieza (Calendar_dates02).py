# Databricks notebook source
#Celda 1
#----------------------
from pyspark.sql import functions as F
dbutils.widgets.text("RAW_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/")
dbutils.widgets.text("CLEAN_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/")
RAW_PATH=dbutils.widgets.get("RAW_PATH"); CLEAN_PATH=dbutils.widgets.get("CLEAN_PATH")

#Lectura desde raw

df = spark.read.format("delta").load(f"{RAW_PATH}calendar_dates")
print("calendar_dates cargado ✅")

#Limpieza

df_clean = (
    df.filter(F.col("service_id").isNotNull())
      .filter(F.col("date").isNotNull())
      .filter(F.col("exception_type").isin(1,2))
      .dropDuplicates(["service_id","date","exception_type"])
      .withColumn("date", F.to_date(F.col("date").cast("string"), "yyyyMMdd"))
      .withColumn("exception_type", F.col("exception_type").cast("int"))
)

#Guardado en zona CLEAN
df_clean.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}calendar_dates")
print("calendar_dates → CLEAN ✅")
dbutils.notebook.exit("OK")