# Databricks notebook: 00_fetch_gtfs_to_raw

import requests, os, zipfile, io
from datetime import datetime

# Widgets 
dbutils.widgets.text("RAW_PATH", "abfss://datos@masterjcmtz002sta.dfs.core.windows.net/raw/", "RAW_PATH")
dbutils.widgets.text("GTFS_URL", "https://sncb-opendata.hafas.de/gtfs/static/c21ac6758dd25af84cca5b707f3cb3de", "GTFS_URL")
RAW_PATH = dbutils.widgets.get("RAW_PATH").strip()
GTFS_URL = dbutils.widgets.get("GTFS_URL").strip()

# 1) Descarga a memoria
print(f"📥 Descargando GTFS desde {GTFS_URL} ...")
resp = requests.get(GTFS_URL, timeout=60)
resp.raise_for_status()

# 2) Descomprime en /dbfs/tmp y luego copia a ABFSS (RAW)
ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
local_dir = f"/dbfs/tmp/gtfs_{ts}"
os.makedirs(local_dir, exist_ok=True)

with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
    zf.extractall(local_dir)

print(f"🗂️  Descomprimido en {local_dir}")

# 3) Sube los .txt a RAW (sobrescribe)
#    Estructura: RAW_PATH/<nombre>.txt
for fname in os.listdir(local_dir):
    if fname.lower().endswith(".txt"):
        src = f"file:{local_dir}/{fname}"
        dst = f"{RAW_PATH}{fname}"
        # si quieres mantener una carpeta por “drop”, usa: f"{RAW_PATH}{ts}/{fname}"
        dbutils.fs.cp(src, dst, True)
        print(f"✅ Copiado {fname} → {dst}")

print("GTFS actualizado en RAW")
dbutils.notebook.exit("OK")
