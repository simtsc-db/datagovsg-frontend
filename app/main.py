"""Singapore Building Management - 3D Map Backend.

FastAPI backend serving traffic camera and taxi location data.
Reads real-time data from data.gov.sg APIs directly, with Lakebase as persistent store.
Deployed as a Databricks App.
"""

import os
import logging
import time
from pathlib import Path

import requests as http_requests
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
CESIUM_API_KEY = os.environ.get("CESIUM_API_KEY", "")
DATAGOV_API_KEY = os.environ.get("DATAGOV_API_KEY", "")

TRAFFIC_IMAGES_URL = "https://api.data.gov.sg/v1/transport/traffic-images"
TAXI_AVAILABILITY_URL = "https://api.data.gov.sg/v1/transport/taxi-availability"

# Simple in-memory cache with TTL
_cache = {}
CACHE_TTL = 30  # seconds


def _cached_fetch(key: str, url: str):
    """Fetch from URL with simple TTL cache."""
    now = time.time()
    if key in _cache and now - _cache[key]["time"] < CACHE_TTL:
        return _cache[key]["data"]

    headers = {}
    if DATAGOV_API_KEY:
        headers["x-api-key"] = DATAGOV_API_KEY

    resp = http_requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    _cache[key] = {"data": data, "time": now}
    return data


app = FastAPI(title="SG Building Management")


@app.get("/api/traffic-cameras")
def get_traffic_cameras():
    """Return all traffic camera data with latest images from data.gov.sg."""
    try:
        data = _cached_fetch("cameras", TRAFFIC_IMAGES_URL)
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
        return {"cameras": cameras, "count": len(cameras)}
    except Exception as e:
        logger.error(f"Error fetching cameras: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/taxi-locations")
def get_taxi_locations():
    """Return all current taxi locations from data.gov.sg."""
    try:
        data = _cached_fetch("taxis", TAXI_AVAILABILITY_URL)
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
        return {"taxis": taxis, "count": len(taxis)}
    except Exception as e:
        logger.error(f"Error fetching taxis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
def health():
    return {"status": "ok"}


# Serve static frontend files
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/")
def serve_index():
    """Serve the main frontend page with Cesium token injected."""
    index_path = Path(__file__).parent / "static" / "index.html"
    if index_path.exists():
        html = index_path.read_text()
        html = html.replace(
            "const CESIUM_TOKEN = window.__CESIUM_TOKEN__ || '';",
            f"const CESIUM_TOKEN = '{CESIUM_API_KEY}';",
        )
        return HTMLResponse(content=html)
    return HTMLResponse(content="<h1>SG Building Management</h1><p>Static files not found.</p>")
