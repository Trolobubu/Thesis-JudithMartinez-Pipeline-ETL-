# Databricks notebook source
# CELDA 1
from pyspark.sql import functions as F


# COMMAND ----------

# CELDA 2
dbutils.widgets.text("RAW_PATH",   "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/")
dbutils.widgets.text("CLEAN_PATH", "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/")
RAW_PATH   = dbutils.widgets.get("RAW_PATH")
CLEAN_PATH = dbutils.widgets.get("CLEAN_PATH")

# COMMAND ----------

# CELDA 3
df = spark.read.format("delta").load(f"{RAW_PATH}agency")
print("agency cargado ✅")

# COMMAND ----------

# CELDA 4
df_clean = (
    df.dropDuplicates(["agency_id"])
      .withColumn("agency_name", F.trim(F.col("agency_name")))
      .withColumn("agency_lang", F.lower(F.col("agency_lang")))
      .drop("agency_phone") 
      
)
print(df_clean)
df_clean.show()

# COMMAND ----------

# CELDA 5
df_clean.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}agency")
print("agency → CLEAN ✅")

# COMMAND ----------

# CELDA 6
dbutils.notebook.exit("OK")