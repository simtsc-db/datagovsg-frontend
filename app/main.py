"""SG PubSec Palantir Clone - 3D Map Backend.

FastAPI backend serving real-time Singapore data from data.gov.sg APIs.
Covers: traffic cameras, taxi availability, HDB carpark occupancy.
Deployed as a Databricks App.
"""

import os
import logging
import math
import time
from pathlib import Path
import csv
import io

import json as json_lib

import requests as http_requests
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
CESIUM_API_KEY = os.environ.get("CESIUM_API_KEY", "")
DATAGOV_API_KEY = os.environ.get("DATAGOV_API_KEY", "")

TRAFFIC_IMAGES_URL = "https://api.data.gov.sg/v1/transport/traffic-images"
TAXI_AVAILABILITY_URL = "https://api.data.gov.sg/v1/transport/taxi-availability"
CARPARK_AVAILABILITY_URL = "https://api.data.gov.sg/v1/transport/carpark-availability"
CARPARK_METADATA_DATASET = "d_23f946fa557947f93a8043bbef41dd09"

# Cache with configurable TTL per key
_cache = {}
CACHE_TTL_SHORT = 30  # seconds for real-time data
CACHE_TTL_LONG = 3600  # seconds for metadata


def _cached_fetch(key: str, url: str, ttl: int = CACHE_TTL_SHORT):
    """Fetch from URL with TTL cache."""
    now = time.time()
    if key in _cache and now - _cache[key]["time"] < ttl:
        return _cache[key]["data"]

    headers = {}
    if DATAGOV_API_KEY:
        headers["x-api-key"] = DATAGOV_API_KEY

    resp = http_requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    _cache[key] = {"data": data, "time": now}
    return data


# --- SVY21 to WGS84 coordinate conversion ---

def _svy21_to_wgs84(northing: float, easting: float) -> tuple[float, float]:
    """Convert SVY21 (x=easting, y=northing) to WGS84 (lat, lon)."""
    a = 6378137.0
    f = 1 / 298.257223563
    oLat = 1.366666
    oLon = 103.833333
    No = 38744.572
    Eo = 28001.642
    k = 0.99999

    b = a * (1 - f)
    e2 = (2 * f) - (f * f)
    e4 = e2 * e2
    e6 = e4 * e2
    A0 = 1 - (e2 / 4) - (3 * e4 / 64) - (5 * e6 / 256)
    A2 = (3.0 / 8.0) * (e2 + (e4 / 4) + (15 * e6 / 128))
    A4 = (15.0 / 256.0) * (e4 + (3 * e6 / 4))
    A6 = 35 * e6 / 3072

    oLatR = oLat * math.pi / 180
    oLonR = oLon * math.pi / 180

    Mo = a * (A0 * oLatR - A2 * math.sin(2 * oLatR) + A4 * math.sin(4 * oLatR) - A6 * math.sin(6 * oLatR))

    Np = northing - No
    Ep = easting - Eo

    Mpr = Mo + Np / k
    n = (a - b) / (a + b)
    n2 = n * n
    n3 = n2 * n
    n4 = n2 * n2

    sigma = (Mpr / a) * (
        (1 + n + (5 * n2 / 4) + (5 * n3 / 4)) /
        (1 + n + (5 * n2 / 4) + (5 * n3 / 4)) if False else 1
    )

    # Iterative approach for footprint latitude
    lat_fp = Mpr / (a * A0)
    for _ in range(10):
        M_calc = a * (A0 * lat_fp - A2 * math.sin(2 * lat_fp) + A4 * math.sin(4 * lat_fp) - A6 * math.sin(6 * lat_fp))
        diff = Mpr - M_calc
        lat_fp += diff / (a * A0)
        if abs(diff) < 1e-10:
            break

    sin_fp = math.sin(lat_fp)
    cos_fp = math.cos(lat_fp)
    tan_fp = math.tan(lat_fp)
    t = tan_fp
    t2 = t * t
    t4 = t2 * t2
    t6 = t4 * t2

    rho = a * (1 - e2) / ((1 - e2 * sin_fp * sin_fp) ** 1.5)
    nu = a / math.sqrt(1 - e2 * sin_fp * sin_fp)
    psi = nu / rho
    psi2 = psi * psi
    psi3 = psi2 * psi
    psi4 = psi2 * psi2

    Ep_nu = Ep / (k * nu)
    Ep_nu2 = Ep_nu * Ep_nu
    Ep_nu3 = Ep_nu2 * Ep_nu
    Ep_nu4 = Ep_nu2 * Ep_nu2
    Ep_nu5 = Ep_nu4 * Ep_nu
    Ep_nu6 = Ep_nu3 * Ep_nu3

    lat = lat_fp \
        - (t / (k * rho)) * (Ep * Ep_nu / 2) \
        + (t / (k * rho)) * (Ep_nu4 * Ep * Ep / 24) * (-4 * psi2 + 9 * psi * (1 - t2) + 12 * t2)

    lon = oLonR + (Ep_nu / cos_fp) \
        - (Ep_nu3 / (6 * cos_fp)) * (psi + 2 * t2) \
        + (Ep_nu5 / (120 * cos_fp)) * (-4 * psi3 * (1 - 6 * t2) + psi2 * (9 - 68 * t2) + 72 * psi * t2 + 24 * t4)

    return lat * 180 / math.pi, lon * 180 / math.pi


# --- Carpark metadata cache ---

_carpark_metadata = None
_carpark_metadata_time = 0


def _get_carpark_metadata() -> dict:
    """Fetch and cache carpark metadata CSV, returns dict keyed by car_park_no."""
    global _carpark_metadata, _carpark_metadata_time
    now = time.time()
    if _carpark_metadata and now - _carpark_metadata_time < CACHE_TTL_LONG:
        return _carpark_metadata

    try:
        # Get download URL
        resp = http_requests.get(
            f"https://api-open.data.gov.sg/v1/public/api/datasets/{CARPARK_METADATA_DATASET}/poll-download",
            timeout=10,
        )
        resp.raise_for_status()
        download_url = resp.json()["data"]["url"]

        # Download CSV
        resp = http_requests.get(download_url, timeout=15)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))

        metadata = {}
        for row in reader:
            cp_no = row.get("car_park_no", "")
            try:
                x = float(row.get("x_coord", 0))
                y = float(row.get("y_coord", 0))
                if x > 0 and y > 0:
                    lat, lon = _svy21_to_wgs84(y, x)
                    metadata[cp_no] = {
                        "address": row.get("address", ""),
                        "latitude": lat,
                        "longitude": lon,
                        "car_park_type": row.get("car_park_type", ""),
                        "free_parking": row.get("free_parking", ""),
                        "night_parking": row.get("night_parking", ""),
                        "car_park_decks": row.get("car_park_decks", ""),
                    }
            except (ValueError, TypeError):
                continue

        _carpark_metadata = metadata
        _carpark_metadata_time = now
        logger.info(f"Loaded carpark metadata: {len(metadata)} entries with coordinates")
        return metadata
    except Exception as e:
        logger.error(f"Failed to load carpark metadata: {e}")
        return _carpark_metadata or {}


app = FastAPI(title="SG PubSec Palantir Clone")


@app.get("/api/traffic-cameras")
def get_traffic_cameras():
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


@app.get("/api/carparks")
def get_carparks():
    """Return carpark availability with location and occupancy data."""
    try:
        metadata = _get_carpark_metadata()
        data = _cached_fetch("carparks", CARPARK_AVAILABILITY_URL)

        carparks = []
        for cp in data.get("items", [{}])[0].get("carpark_data", []):
            cp_no = cp.get("carpark_number", "")
            meta = metadata.get(cp_no)
            if not meta:
                continue

            # Aggregate lots across all lot types
            total = 0
            available = 0
            for info in cp.get("carpark_info", []):
                total += int(info.get("total_lots", 0))
                available += int(info.get("lots_available", 0))

            if total == 0:
                continue

            occupancy = (total - available) / total
            carparks.append({
                "carpark_number": cp_no,
                "address": meta["address"],
                "latitude": meta["latitude"],
                "longitude": meta["longitude"],
                "car_park_type": meta["car_park_type"],
                "free_parking": meta["free_parking"],
                "night_parking": meta["night_parking"],
                "total_lots": total,
                "available_lots": available,
                "occupancy": round(occupancy, 3),
                "updated_at": cp.get("update_datetime", ""),
            })

        return {"carparks": carparks, "count": len(carparks)}
    except Exception as e:
        logger.error(f"Error fetching carparks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class ChatRequest(BaseModel):
    message: str


# Well-known Singapore landmarks -> search keywords for carpark matching
_LANDMARK_KEYWORDS = {
    "sentosa": ["sentosa", "harbourfront", "telok blangah"],
    "raffles place": ["raffles", "cecil", "robinson", "shenton"],
    "orchard": ["orchard", "somerset", "scotts"],
    "marina bay": ["marina", "bayfront", "shenton"],
    "changi": ["changi", "airport"],
    "jurong": ["jurong"],
    "tampines": ["tampines"],
    "woodlands": ["woodlands"],
    "ang mo kio": ["ang mo kio"],
    "toa payoh": ["toa payoh"],
    "bedok": ["bedok"],
    "clementi": ["clementi"],
    "bishan": ["bishan"],
    "bukit timah": ["bukit timah"],
    "chinatown": ["chinatown", "kreta ayer", "outram"],
    "little india": ["little india", "serangoon", "farrer park"],
    "bugis": ["bugis", "albert", "victoria"],
    "hougang": ["hougang"],
    "punggol": ["punggol"],
    "sengkang": ["sengkang"],
    "yishun": ["yishun"],
    "pasir ris": ["pasir ris"],
    "bukit merah": ["bukit merah"],
    "queenstown": ["queenstown"],
    "dover": ["dover"],
    "novena": ["novena"],
    "newton": ["newton"],
    "kallang": ["kallang"],
    "geylang": ["geylang"],
    "eunos": ["eunos"],
    "simei": ["simei"],
    "bukit batok": ["bukit batok"],
    "bukit panjang": ["bukit panjang"],
    "choa chu kang": ["choa chu kang"],
    "serangoon": ["serangoon"],
    "potong pasir": ["potong pasir"],
    "macpherson": ["macpherson"],
}


def _search_carparks(query: str) -> str:
    """Search carparks by location keywords from the query."""
    query_lower = query.lower()

    # Find matching location keywords
    search_terms = []
    for landmark, keywords in _LANDMARK_KEYWORDS.items():
        if landmark in query_lower:
            search_terms.extend(keywords)
            break

    # If no landmark matched, extract nouns/words as search terms
    if not search_terms:
        stop_words = {"which", "what", "where", "are", "is", "the", "near", "in", "at", "how", "many",
                      "free", "available", "full", "empty", "carpark", "carparks", "car", "park", "parking",
                      "lot", "lots", "can", "i", "find", "show", "me", "right", "now", "current", "currently",
                      "there", "any", "a", "an", "to", "of", "and", "from", "around", "close"}
        words = [w.strip("?.,!") for w in query_lower.split() if w.strip("?.,!") not in stop_words and len(w) > 2]
        search_terms = words

    if not search_terms:
        return None

    try:
        cp_data = get_carparks()
        matches = []
        for cp in cp_data["carparks"]:
            addr = cp["address"].lower()
            if any(term in addr for term in search_terms):
                matches.append(cp)

        if not matches:
            return f"No carparks found matching '{' '.join(search_terms)}'. Try a different area name."

        # Sort by available lots (most free first)
        matches.sort(key=lambda c: c["occupancy"])

        lines = [f"Found {len(matches)} carpark(s) near {search_terms[0].title()}:\n"]
        for cp in matches[:10]:
            pct = round(cp["occupancy"] * 100)
            status = "FULL" if pct >= 95 else "Nearly full" if pct >= 80 else "Available" if pct < 50 else "Moderate"
            lines.append(f"  {cp['address']}: {cp['available_lots']}/{cp['total_lots']} lots free ({pct}% full) - {status}")

        if len(matches) > 10:
            lines.append(f"  ... and {len(matches) - 10} more")

        return "\n".join(lines)
    except Exception as e:
        return f"Error searching carparks: {e}"


def _answer_query(query: str) -> str:
    """Answer a natural language question using live data."""
    q = query.lower()

    # Taxi queries
    if "taxi" in q:
        try:
            data = get_taxi_locations()
            count = data["count"]
            if "how many" in q or "count" in q or "number" in q:
                return f"There are currently {count:,} taxis available across Singapore. Taxi density is approximately {count/730:.1f} per km\u00B2."
            elif any(area in q for area in _LANDMARK_KEYWORDS):
                return f"There are {count:,} taxis currently available island-wide. The data provides total taxi count ({count:,}) but not per-area breakdown. The taxis are visible on the map as blue dots - zoom into your area of interest."
            else:
                return f"Currently {count:,} taxis are available across Singapore (density: {count/730:.1f}/km\u00B2). The real-time positions are shown as blue markers on the map."
        except Exception as e:
            return f"Error fetching taxi data: {e}"

    # Camera queries
    if "camera" in q:
        try:
            data = get_traffic_cameras()
            return f"There are {data['count']} active LTA traffic cameras across Singapore, shown as red dots on the map. Click any camera marker to view its live image."
        except Exception as e:
            return f"Error fetching camera data: {e}"

    # Carpark queries
    if any(w in q for w in ["carpark", "car park", "parking", "park", "free", "full", "empty", "lot"]):
        # Try location search first
        result = _search_carparks(query)
        if result:
            return result

        # General carpark stats
        try:
            data = get_carparks()
            total_lots = sum(c["total_lots"] for c in data["carparks"])
            total_avail = sum(c["available_lots"] for c in data["carparks"])
            avg_occ = round((total_lots - total_avail) / total_lots * 100) if total_lots else 0
            full = sum(1 for c in data["carparks"] if c["occupancy"] >= 0.95)
            empty = sum(1 for c in data["carparks"] if c["occupancy"] <= 0.2)
            return (
                f"Tracking {data['count']:,} HDB carparks with {total_lots:,} total lots ({total_avail:,} available). "
                f"Average occupancy: {avg_occ}%. "
                f"{full} carparks are full (>95%), {empty} are nearly empty (<20%). "
                f"Carparks are shown as colored cylinders on the map (green=empty, red=full)."
            )
        except Exception as e:
            return f"Error fetching carpark data: {e}"

    # Location-based queries (try carpark search as default)
    for landmark in _LANDMARK_KEYWORDS:
        if landmark in q:
            result = _search_carparks(query)
            if result:
                return result

    # General/overview queries
    if any(w in q for w in ["overview", "summary", "status", "dashboard", "everything", "all"]):
        parts = []
        try:
            parts.append(f"Taxis: {get_taxi_locations()['count']:,} available")
        except Exception:
            pass
        try:
            parts.append(f"Cameras: {get_traffic_cameras()['count']} active")
        except Exception:
            pass
        try:
            cp = get_carparks()
            avg = round(sum(c["occupancy"] for c in cp["carparks"]) / len(cp["carparks"]) * 100) if cp["carparks"] else 0
            parts.append(f"Carparks: {cp['count']:,} tracked, {avg}% avg occupancy")
        except Exception:
            pass
        return "Current Singapore overview:\n" + "\n".join(f"  {p}" for p in parts)

    return "I can help with questions about Singapore's live data: taxi availability, traffic cameras, and HDB carpark occupancy. Try asking things like 'How many taxis are available?', 'Which carparks near Orchard are free?', or 'Show me parking near Tampines'."


@app.post("/api/chat")
def chat(req: ChatRequest):
    """Answer natural language questions about live Singapore data."""
    try:
        db_host = os.environ.get("DATABRICKS_HOST", "")
        if db_host and not db_host.startswith("http"):
            db_host = f"https://{db_host}"
        db_token = os.environ.get("DATABRICKS_TOKEN", "")

        # Try Databricks Foundation Model API first
        if db_host and db_token:
            try:
                # Build compact context (just stats + matched carparks for efficiency)
                taxi_count = get_taxi_locations()["count"]
                cam_count = get_traffic_cameras()["count"]
                cp_data = get_carparks()
                total_avail = sum(c["available_lots"] for c in cp_data["carparks"])
                avg_occ = round(sum(c["occupancy"] for c in cp_data["carparks"]) / len(cp_data["carparks"]) * 100)

                # Find relevant carparks based on query keywords
                q_lower = req.message.lower()
                relevant_cps = []
                for landmark, kws in _LANDMARK_KEYWORDS.items():
                    if landmark in q_lower:
                        for cp in cp_data["carparks"]:
                            if any(kw in cp["address"].lower() for kw in kws):
                                relevant_cps.append(cp)
                        break

                context = (
                    f"Live data: {taxi_count:,} taxis, {cam_count} cameras, {cp_data['count']:,} carparks "
                    f"({total_avail:,} lots available, {avg_occ}% avg occupancy)."
                )
                if relevant_cps:
                    context += "\nRelevant carparks:\n"
                    for c in sorted(relevant_cps, key=lambda x: x["occupancy"])[:15]:
                        context += f"  {c['address']}: {c['available_lots']}/{c['total_lots']} free ({round(c['occupancy']*100)}% full)\n"

                messages = [
                    {"role": "system", "content": (
                        "You are the AI assistant for SG PubSec Palantir Clone, a Singapore real-time monitoring dashboard. "
                        "Answer concisely (2-4 sentences). Use the live data provided.\n\n" + context
                    )},
                    {"role": "user", "content": req.message},
                ]

                resp = http_requests.post(
                    f"{db_host.rstrip('/')}/serving-endpoints/databricks-meta-llama-3-3-70b-instruct/invocations",
                    headers={"Authorization": f"Bearer {db_token}"},
                    json={"messages": messages, "max_tokens": 300, "temperature": 0.3},
                    timeout=25,
                )
                resp.raise_for_status()
                answer = resp.json()["choices"][0]["message"]["content"]
                return {"answer": answer}
            except Exception as llm_err:
                logger.warning(f"LLM unavailable ({llm_err}), falling back to local search")

        # Fallback: local data search
        answer = _answer_query(req.message)
        return {"answer": answer}
    except Exception as e:
        logger.error(f"Chat error: {e}")
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
    index_path = Path(__file__).parent / "static" / "index.html"
    if index_path.exists():
        html = index_path.read_text()
        html = html.replace(
            "const CESIUM_TOKEN = window.__CESIUM_TOKEN__ || '';",
            f"const CESIUM_TOKEN = '{CESIUM_API_KEY}';",
        )
        return HTMLResponse(content=html)
    return HTMLResponse(content="<h1>SG PubSec Palantir Clone</h1><p>Static files not found.</p>")
