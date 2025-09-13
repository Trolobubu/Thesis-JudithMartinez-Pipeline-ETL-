# Databricks notebook source
#Celda 1
from pyspark.sql import functions as F
dbutils.widgets.text("RAW_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/")
dbutils.widgets.text("CLEAN_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/")
RAW_PATH=dbutils.widgets.get("RAW_PATH"); CLEAN_PATH=dbutils.widgets.get("CLEAN_PATH")

df = spark.read.format("delta").load(f"{RAW_PATH}calendar")
print("calendar cargado ✅")

df_clean = (
    df.filter(F.col("service_id").isNotNull())
      .dropDuplicates(["service_id"])
      .withColumn("monday",    F.col("monday").cast("int"))
      .withColumn("tuesday",   F.col("tuesday").cast("int"))
      .withColumn("wednesday", F.col("wednesday").cast("int"))
      .withColumn("thursday",  F.col("thursday").cast("int"))
      .withColumn("friday",    F.col("friday").cast("int"))
      .withColumn("saturday",  F.col("saturday").cast("int"))
      .withColumn("sunday",    F.col("sunday").cast("int"))
      .withColumn("start_date", F.to_date(F.col("start_date").cast("string"), "yyyyMMdd"))
      .withColumn("end_date",   F.to_date(F.col("end_date").cast("string"), "yyyyMMdd"))
      .filter(F.col("end_date") >= F.col("start_date"))
)

df_clean.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}calendar")
print("calendar → CLEAN ✅")
dbutils.notebook.exit("OK")