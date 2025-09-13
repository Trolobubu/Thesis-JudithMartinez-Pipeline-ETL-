# Databricks notebook source
#Celda 1
#----------------------

from pyspark.sql import functions as F
# Widgets

dbutils.widgets.text("RAW_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/")
dbutils.widgets.text("CLEAN_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/")
RAW_PATH=dbutils.widgets.get("RAW_PATH"); CLEAN_PATH=dbutils.widgets.get("CLEAN_PATH")

df = spark.read.format("delta").load(f"{RAW_PATH}routes")
print("routes cargado ✅")

df_clean = (
    df.filter(F.col("route_id").isNotNull())
      .dropDuplicates(["route_id"])
      .withColumn("route_type", F.col("route_type").cast("int"))
      .withColumn("route_short_name", F.trim(F.col("route_short_name")))
      .withColumn("route_long_name",  F.trim(F.col("route_long_name")))
      # Opcional: limitar dominio route_type
      # .filter(F.col("route_type").isin(0,1,2,3,4,5,6,7,11,12))
)

df_clean.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}routes")
print("routes → CLEAN ✅")
dbutils.notebook.exit("OK")