# Databricks notebook source
#Celda 1
#----------------------
from pyspark.sql import functions as F

# COMMAND ----------

#Celda 2
#----------------------
dbutils.widgets.text("RAW_PATH",   "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/")
dbutils.widgets.text("CLEAN_PATH", "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/")
# opcional: ENV, fecha, etc.

RAW_PATH   = dbutils.widgets.get("RAW_PATH")
CLEAN_PATH = dbutils.widgets.get("CLEAN_PATH")

# COMMAND ----------

#Celda 3
#----------------------
# lectura de trips desde RAW 
tripsDF = spark.read.format("delta").load(f"{RAW_PATH}trips")
print("trips cargado ✅")

# COMMAND ----------


#Celda 4
#----------------------
# limpieza 
trips_clean = (
    tripsDF
    .filter(F.col("trip_id").isNotNull())
    .filter(F.col("route_id").isNotNull())
    .filter(F.col("service_id").isNotNull())
    .dropDuplicates(["trip_id"])
    .withColumn("direction_id", F.col("direction_id").cast("int"))
)

# COMMAND ----------

#Celda 5
#---------------------- 
# escritura en CLEAN

trips_clean.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}trips")
print("trips → CLEAN ✅")

# COMMAND ----------

#Celda 6
#---------------------- 
#salida para el Job
dbutils.notebook.exit("OK")