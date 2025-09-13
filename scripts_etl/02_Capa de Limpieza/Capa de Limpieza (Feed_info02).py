# Databricks notebook source
#Celda 1
#-----------------------------------------------
from pyspark.sql import functions as F

# Widgets
dbutils.widgets.text("RAW_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/")
dbutils.widgets.text("CLEAN_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/")
RAW_PATH=dbutils.widgets.get("RAW_PATH"); CLEAN_PATH=dbutils.widgets.get("CLEAN_PATH")

# Lectura desde RAW

df = spark.read.format("delta").load(f"{RAW_PATH}feed_info")
print("feed_info cargado ✅")

# Limpieza

df_clean = (
    df.dropDuplicates()
      .withColumn("feed_publisher_name", F.trim(F.col("feed_publisher_name")))
      .withColumn("feed_lang", F.lower(F.col("feed_lang")))
      .withColumn("feed_start_date", F.to_date(F.col("feed_start_date").cast("string"), "yyyyMMdd"))
      .withColumn("feed_end_date",   F.to_date(F.col("feed_end_date").cast("string"), "yyyyMMdd"))
)
# Guardado en zona CLEAN 
df_clean.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}feed_info")
print("feed_info → CLEAN ✅")
dbutils.notebook.exit("OK")