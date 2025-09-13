# Databricks notebook source
#Celda 1
#----------------------

from pyspark.sql import functions as F

# Widgets
dbutils.widgets.text("RAW_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/")
dbutils.widgets.text("CLEAN_PATH","abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/")
RAW_PATH=dbutils.widgets.get("RAW_PATH"); CLEAN_PATH=dbutils.widgets.get("CLEAN_PATH")

# Leer stop_times desde RAW

df = spark.read.format("delta").load(f"{RAW_PATH}stop_times")
print("stop_times cargado ✅")

# Limpieza
df_clean = (
    df.filter(F.col("trip_id").isNotNull())
      .filter(F.col("stop_id").isNotNull())
      .filter(F.col("stop_sequence").isNotNull())
      .dropDuplicates(["trip_id","stop_sequence"])
      .withColumn("stop_sequence", F.col("stop_sequence").cast("int"))
)

regex_hms = r"^([0-9]{1,2}|[0-9]{2}):([0-5][0-9]):([0-5][0-9])$"  # acepta >24h
df_clean = df_clean.filter(
    (F.col("arrival_time").isNull()   | F.col("arrival_time").rlike(regex_hms)) &
    (F.col("departure_time").isNull() | F.col("departure_time").rlike(regex_hms))
)
# Guardado en zona CLEAN 
df_clean.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}stop_times")
print("stop_times → CLEAN ✅")
dbutils.notebook.exit("OK")