# Databricks notebook source
# 03_load_to_cosmos — Carga de tablas CLEAN a Azure Cosmos DB (API MongoDB)
# Requiere librería de cluster: org.mongodb.spark:mongo-spark-connector_2.12:10.3.0

from pyspark.sql.functions import sha2, concat_ws, col

#  Configuración fija 
CLEAN_PATH = "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/clean/"
DB_NAME    = "gtfs"

COSMOS_URI = (
    "mongodb://nmbs:"
    "SOoIPsiFh3yOrfc7QJ6X0XSGKCWmTwyyKFIiPZAEch96EXxjI0fGc8nFKbCoziE6VFE2xHEb85ulACDbX8YbXg=="
    "@nmbs.mongo.cosmos.azure.com:10255/"
    "?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@nmbs@"
)

print("→ Conexión a Cosmos configurada (URI incluida).")

# Claves por tabla para _id determinista 
KEYS = {
    "routes": ["route_id"],
    "trips": ["trip_id"],
    "stop_times": ["trip_id","stop_id","stop_sequence"],
    "calendar": ["service_id"],
    "calendar_dates": ["service_id","date"],
    "transfers": ["from_stop_id","to_stop_id"],
    "agency": ["agency_id"],
    "stops": ["stop_id"],
    "feed_info": ["feed_publisher_name","feed_version"],
    "translations": ["table_name","field_name","language","record_id"],
    "stop_time_overrides": ["trip_id","stop_id","stop_sequence"],
}

def add_id(df, table):
    keys = KEYS.get(table, [])
    if keys and all(k in df.columns for k in keys):
        return df.withColumn("_id", sha2(concat_ws(":", *[col(k).cast("string") for k in keys]), 256))
    return df.withColumn("_id", sha2(concat_ws(":", *[col(c).cast("string") for c in df.columns]), 256))

# Prueba mínima (smoke test) 
test_df = spark.createDataFrame([(1, "ok"), (2, "cosmos")], ["_id", "msg"]).coalesce(1)

(test_df.write
   .format("mongodb")
   .mode("overwrite")
   .option("connection.uri", COSMOS_URI)
   .option("database", DB_NAME)
   .option("collection", "smoke_test")
   .option("replaceDocument", "true")
   .option("maxBatchSize", "100")
   .save())
print(f"✅ Smoke test escrito en Cosmos ({DB_NAME}.smoke_test)")

#  Tablas CLEAN para publicar
TABLES = [
    "agency","calendar","calendar_dates","feed_info","routes",
    "stop_times","stop_time_overrides","stops","trips","transfers","translations"
]

#  Escritura por tabla 
for t in TABLES:
    try:
        print(f"→ Procesando {t} ...")
        df = spark.read.format("delta").load(f"{CLEAN_PATH}{t}")

        # Limpieza de nombres de columnas
        df = df.toDF(*[c.replace(".", "_").lstrip("$") for c in df.columns])

        # Añadir _id determinista
        df = add_id(df, t)

        (df.coalesce(1)
           .write
           .format("mongodb")
           .mode("overwrite")
           .option("connection.uri", COSMOS_URI)
           .option("database", DB_NAME)
           .option("collection", t)
           .option("replaceDocument", "true")
           .option("maxBatchSize", "100")
           .save())

        print(f"✅ {t} → Cosmos ({DB_NAME}.{t})")

    except Exception as e:
        print(f"❌ {t}: error → {e}")

print(" Publicación completa en Azure Cosmos DB (API MongoDB)")
dbutils.notebook.exit("OK")
