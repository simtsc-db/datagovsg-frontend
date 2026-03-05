# Databricks notebook source
# MAGIC %md
# MAGIC # Singapore Traffic Data Ingestion Pipeline
# MAGIC Fetches real-time traffic camera images and taxi availability from data.gov.sg,
# MAGIC writes to Lakebase (primary storage), then syncs Delta tables from Lakebase.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import requests
import json
import psycopg2
import psycopg2.extras
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

DATAGOV_API_KEY = dbutils.secrets.get(scope="simtsc", key="datagov-api-key")
LAKEBASE_HOST = dbutils.secrets.get(scope="simtsc", key="lakebase-host")
CATALOG = "serverless_sandbox_simtsc_new_catalog"
SCHEMA = "sg_building"
LAKEBASE_DB = "sg_building"

TRAFFIC_IMAGES_URL = "https://api.data.gov.sg/v1/transport/traffic-images"
TAXI_AVAILABILITY_URL = "https://api.data.gov.sg/v1/transport/taxi-availability"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Lakebase Credentials

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_token = ctx.apiToken().get()
api_url = ctx.apiUrl().get()
user_email = spark.sql("SELECT current_user()").collect()[0][0]

resp = requests.post(
    f"{api_url}/api/2.0/postgres/credentials",
    headers={"Authorization": f"Bearer {api_token}"},
    json={"endpoint": "projects/sg-building/branches/production/endpoints/primary"},
)
resp.raise_for_status()
pg_token = resp.json()["token"]
print(f"Lakebase credential acquired for {user_email}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch & Write to Lakebase (Primary Storage)

# COMMAND ----------

def fetch_traffic_images():
    resp = requests.get(TRAFFIC_IMAGES_URL, headers={"x-api-key": DATAGOV_API_KEY})
    resp.raise_for_status()
    data = resp.json()
    cameras = []
    for item in data.get("items", []):
        for cam in item.get("cameras", []):
            cameras.append({
                "camera_id": cam["camera_id"],
                "image_url": cam["image"],
                "image_width": cam.get("image_metadata", {}).get("width"),
                "image_height": cam.get("image_metadata", {}).get("height"),
                "latitude": cam["location"]["latitude"],
                "longitude": cam["location"]["longitude"],
                "captured_at": cam["timestamp"],
            })
    return cameras

def fetch_taxi_locations():
    resp = requests.get(TAXI_AVAILABILITY_URL, headers={"x-api-key": DATAGOV_API_KEY})
    resp.raise_for_status()
    data = resp.json()
    taxis = []
    for feature in data.get("features", []):
        coords = feature["geometry"]["coordinates"]
        timestamp = feature["properties"]["timestamp"]
        for coord in coords:
            taxis.append({
                "longitude": coord[0],
                "latitude": coord[1],
                "captured_at": timestamp,
            })
    return taxis

cameras = fetch_traffic_images()
taxis = fetch_taxi_locations()
print(f"Fetched {len(cameras)} cameras, {len(taxis)} taxis from data.gov.sg")

# COMMAND ----------

# Write to Lakebase (primary storage)
conn = psycopg2.connect(
    host=LAKEBASE_HOST, port=5432, dbname=LAKEBASE_DB,
    user=user_email, password=pg_token, sslmode="require", connect_timeout=10,
)
cur = conn.cursor()

# Upsert cameras
cur.execute("DELETE FROM traffic_cameras")
for cam in cameras:
    cur.execute("""
        INSERT INTO traffic_cameras (camera_id, image_url, image_width, image_height, latitude, longitude, captured_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    """, (cam["camera_id"], cam["image_url"], cam["image_width"], cam["image_height"],
          cam["latitude"], cam["longitude"], cam["captured_at"]))
conn.commit()
print(f"Wrote {len(cameras)} cameras to Lakebase")

# Upsert taxis
cur.execute("DELETE FROM taxi_locations")
for taxi in taxis:
    cur.execute("""
        INSERT INTO taxi_locations (latitude, longitude, captured_at, updated_at)
        VALUES (%s, %s, %s, NOW())
    """, (taxi["latitude"], taxi["longitude"], taxi["captured_at"]))
conn.commit()
print(f"Wrote {len(taxis)} taxis to Lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Delta Tables from Lakebase

# COMMAND ----------

# Read back from Lakebase and write to Delta
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Sync cameras to Delta
cur.execute("SELECT camera_id, image_url, image_width, image_height, latitude, longitude, captured_at, updated_at FROM traffic_cameras")
camera_rows = cur.fetchall()

camera_schema = StructType([
    StructField("camera_id", StringType()),
    StructField("image_url", StringType()),
    StructField("image_width", IntegerType()),
    StructField("image_height", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("captured_at", StringType()),
    StructField("updated_at", StringType()),
])
df_cameras = spark.createDataFrame([dict(r) for r in camera_rows], schema=camera_schema)
df_cameras = df_cameras.withColumn("captured_at", F.to_timestamp("captured_at"))
df_cameras = df_cameras.withColumn("updated_at", F.to_timestamp("updated_at"))
df_cameras.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.traffic_cameras")
print(f"Synced {len(camera_rows)} cameras to Delta: {CATALOG}.{SCHEMA}.traffic_cameras")

# COMMAND ----------

# Sync taxis to Delta
cur.execute("SELECT id, latitude, longitude, captured_at, updated_at FROM taxi_locations")
taxi_rows = cur.fetchall()

taxi_schema = StructType([
    StructField("id", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("captured_at", StringType()),
    StructField("updated_at", StringType()),
])
df_taxis = spark.createDataFrame([dict(r) for r in taxi_rows], schema=taxi_schema)
df_taxis = df_taxis.withColumn("captured_at", F.to_timestamp("captured_at"))
df_taxis = df_taxis.withColumn("updated_at", F.to_timestamp("updated_at"))
df_taxis.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.taxi_locations")
print(f"Synced {len(taxi_rows)} taxis to Delta: {CATALOG}.{SCHEMA}.taxi_locations")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=== Ingestion Complete ===")
print(f"Traffic cameras: {len(cameras)}")
print(f"Taxi locations: {len(taxis)}")
print(f"Lakebase (primary): {LAKEBASE_DB}")
print(f"Delta (synced): {CATALOG}.{SCHEMA}.traffic_cameras, {CATALOG}.{SCHEMA}.taxi_locations")
