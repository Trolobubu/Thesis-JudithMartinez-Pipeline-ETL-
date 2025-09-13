# Databricks notebook source
# Capa de ingesta y escritura. 

import pyspark.sql.functions as F 
files= [
    "agency","calendar","calendar_dates","feed_info","routes","stop_times","stop_time_overrides","stops","trips","transfers","translations"
] 
dfs = {}
base_path =  "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/"
raw_path  = "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/"

dbutils.fs.rm(raw_path, recurse=True) #Borra todo dentro del path, la práctica esponerlo fuera

for file in files: 
    df_name = file + "DF"   
    df=spark.read\
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("escape", "\"") \
        .option("mode", "DROPMALFORMED") \
       .csv(base_path + file + ".txt") 
    dfs[df_name]=df 
    count_rows = df.count()
    print(f"{df_name} → {count_rows} filas leídas ✅")
    df.write.format("delta").mode("overwrite").save(raw_path + file)

print("✅ Ingesta completada y guardada en zona RAW")

# COMMAND ----------

# MAGIC %md
# MAGIC Zona de Pruebas

# COMMAND ----------


#Pruebas
for name, df in dfs.items():
    print(f"Mostrando todas las filas de {name}:")
    df.show(truncate=False, n=df.count())
    

# COMMAND ----------

# MAGIC %md
# MAGIC #Leer los datos dsede la zona raw 

# COMMAND ----------


raw_path = "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/"
files = ["agency","calendar","calendar_dates","feed_info","routes","stop_times","stop_time_overrides","stops","trips","transfers","translations"
]
dfs_raw= {}
for file in files: 
    df = spark.read.format("delta").load(f"{raw_path}{file}")
    dfs_raw[file]=df
    count_rows = df.count()
    print(f"{file} → {count_rows} filas leídas ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC Zona de pruebas
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls(raw_path))

# COMMAND ----------

dfs_raw['agency'].show()

# COMMAND ----------

dfs_raw['stops'].display()

# COMMAND ----------

dfs_raw['stop_times'].show()

# COMMAND ----------

dfs_raw['calendar'].show()
