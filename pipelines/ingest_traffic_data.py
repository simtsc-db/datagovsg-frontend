# Databricks notebook source
# MAGIC %md
# MAGIC # Singapore Traffic Data Ingestion Pipeline
# MAGIC Continuously fetches real-time traffic camera images and taxi availability from data.gov.sg,
# MAGIC writes to Lakebase (primary storage), then syncs Delta tables from Lakebase.
# MAGIC Runs in a loop with 60-second intervals.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import requests
import json
import time
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

INTERVAL_SECONDS = 60

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def get_lakebase_connection():
    """Get authenticated Lakebase connection."""
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

    return psycopg2.connect(
        host=LAKEBASE_HOST, port=5432, dbname=LAKEBASE_DB,
        user=user_email, password=pg_token, sslmode="require", connect_timeout=10,
    )


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


def write_to_lakebase(conn, cameras, taxis):
    """Write data to Lakebase (primary storage)."""
    cur = conn.cursor()

    cur.execute("DELETE FROM traffic_cameras")
    for cam in cameras:
        cur.execute("""
            INSERT INTO traffic_cameras (camera_id, image_url, image_width, image_height, latitude, longitude, captured_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        """, (cam["camera_id"], cam["image_url"], cam["image_width"], cam["image_height"],
              cam["latitude"], cam["longitude"], cam["captured_at"]))
    conn.commit()

    cur.execute("DELETE FROM taxi_locations")
    for taxi in taxis:
        cur.execute("""
            INSERT INTO taxi_locations (latitude, longitude, captured_at, updated_at)
            VALUES (%s, %s, %s, NOW())
        """, (taxi["latitude"], taxi["longitude"], taxi["captured_at"]))
    conn.commit()
    cur.close()


def sync_to_delta(conn):
    """Sync Lakebase data to Delta tables."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Cameras
    cur.execute("SELECT camera_id, image_url, image_width, image_height, latitude, longitude, captured_at, updated_at FROM traffic_cameras")
    camera_rows = cur.fetchall()
    camera_schema = StructType([
        StructField("camera_id", StringType()), StructField("image_url", StringType()),
        StructField("image_width", IntegerType()), StructField("image_height", IntegerType()),
        StructField("latitude", DoubleType()), StructField("longitude", DoubleType()),
        StructField("captured_at", StringType()), StructField("updated_at", StringType()),
    ])
    df = spark.createDataFrame([dict(r) for r in camera_rows], schema=camera_schema)
    df = df.withColumn("captured_at", F.to_timestamp("captured_at")).withColumn("updated_at", F.to_timestamp("updated_at"))
    df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.traffic_cameras")

    # Taxis
    cur.execute("SELECT id, latitude, longitude, captured_at, updated_at FROM taxi_locations")
    taxi_rows = cur.fetchall()
    taxi_schema = StructType([
        StructField("id", IntegerType()), StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()), StructField("captured_at", StringType()),
        StructField("updated_at", StringType()),
    ])
    df = spark.createDataFrame([dict(r) for r in taxi_rows], schema=taxi_schema)
    df = df.withColumn("captured_at", F.to_timestamp("captured_at")).withColumn("updated_at", F.to_timestamp("updated_at"))
    df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.taxi_locations")

    cur.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous Ingestion Loop

# COMMAND ----------

iteration = 0
while True:
    iteration += 1
    start = time.time()
    try:
        # Fetch from APIs
        cameras = fetch_traffic_images()
        taxis = fetch_taxi_locations()
        print(f"[{iteration}] Fetched {len(cameras)} cameras, {len(taxis)} taxis")

        # Write to Lakebase
        conn = get_lakebase_connection()
        write_to_lakebase(conn, cameras, taxis)
        print(f"[{iteration}] Lakebase updated")

        # Sync to Delta
        sync_to_delta(conn)
        conn.close()
        print(f"[{iteration}] Delta synced")

        elapsed = time.time() - start
        print(f"[{iteration}] Completed in {elapsed:.1f}s")

    except Exception as e:
        print(f"[{iteration}] ERROR: {e}")

    # Sleep until next interval
    elapsed = time.time() - start
    sleep_time = max(0, INTERVAL_SECONDS - elapsed)
    if sleep_time > 0:
        print(f"[{iteration}] Sleeping {sleep_time:.0f}s until next cycle")
        time.sleep(sleep_time)
