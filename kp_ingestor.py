import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

import requests
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

# ============================================================
# HARDCODED CONFIG (as requested)
# ============================================================

# GFZ Kp API base (from GFZ docs example)
GFZ_KP_API_URL = "https://kp.gfz.de/app/json/"

# What to fetch
KP_INDEX = "Kp"          # e.g., Kp, ap, Ap, Cp, C9, Hp30, Hp60
ONLY_DEFINITIVE = False  # True -> add status=def (only definitive values, if supported)
LOOKBACK_HOURS = 6      # fetch last 24h on each poll (safe for missed runs)
POLL_INTERVAL_SECONDS = 10  # 24 hours

# MongoDB details
MONGO_URI = "mongodb+srv://debojyotimahapatra_db_user:NM2K3z8HGe3qWJjZ@cluster0.7ejimre.mongodb.net/?appName=Cluster0"
MONGO_DB_NAME = "cosmo_project"
MONGO_COLLECTION_NAME = "kp_data"

LOG_LEVEL = "INFO"
REQUEST_TIMEOUT_SECONDS = 20

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================
# MongoDB setup
# ============================================================
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
db = mongo_client[MONGO_DB_NAME]
collection = db[MONGO_COLLECTION_NAME]

try:
    collection.create_index("received_at_utc")
    collection.create_index("source_time_utc")
    collection.create_index("index_name")
    collection.create_index("dedupe_key", unique=True)
except PyMongoError as e:
    logger.warning("Could not create indexes: %s", e)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_api_time(dt: datetime) -> str:
    """
    Format datetime as GFZ example style: YYYY-MM-DDTHH:MM:SSZ
    """
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_utc(value: str):
    """
    Parse common UTC timestamp forms to aware UTC datetime.
    """
    if not value:
        return None
    s = value.strip()
    # normalize trailing Z for Python fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fetch_kp_data(index_name: str, start_dt: datetime, end_dt: datetime, only_definitive: bool = False) -> Any:
    """
    Calls GFZ JSON API. The docs show query params start, end, index and optional status=def.
    """
    params = {
        "start": to_api_time(start_dt),
        "end": to_api_time(end_dt),
        "index": index_name,
    }
    if only_definitive:
        params["status"] = "def"

    logger.info("Requesting GFZ Kp API: index=%s start=%s end=%s%s",
                index_name, params["start"], params["end"],
                " status=def" if only_definitive else "")

    resp = requests.get(GFZ_KP_API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    resp.raise_for_status()

    # GFZ page says JSON API; parse JSON.
    return resp.json()


def normalize_gfz_response(raw: Any, requested_index: str) -> List[Dict[str, Any]]:
    """
    Make the script robust to shape differences. GFZ examples/documentation describe time/value/status data
    but exact JSON structure can vary across clients/indices.

    We normalize into a list of records:
      - source_time_utc
      - value
      - status
      - index_name
      - source_payload (per-item raw)
    """
    rows: List[Dict[str, Any]] = []

    # Case 1: API returns list of dicts directly
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                rows.append(item)
        return [normalize_item(r, requested_index) for r in rows]

    # Case 2: API returns dict with arrays or records
    if isinstance(raw, dict):
        # Common possibilities:
        # {"time":[...], "Kp":[...], "status":[...]}
        keys_lower = {str(k).lower(): k for k in raw.keys()}

        time_key = None
        for k in raw.keys():
            if str(k).lower() in ("time", "times", "datetime", "date"):
                time_key = k
                break

        # Try direct index key match (case-insensitive)
        value_key = None
        for k in raw.keys():
            if str(k).lower() == requested_index.lower():
                value_key = k
                break

        status_key = None
        for k in raw.keys():
            if str(k).lower() == "status":
                status_key = k
                break

        if time_key and value_key and isinstance(raw[time_key], list) and isinstance(raw[value_key], list):
            time_arr = raw[time_key]
            val_arr = raw[value_key]
            status_arr = raw.get(status_key, []) if status_key else []

            n = min(len(time_arr), len(val_arr))
            for i in range(n):
                rows.append({
                    "time": time_arr[i],
                    "value": val_arr[i],
                    "status": status_arr[i] if i < len(status_arr) else None,
                    "_raw_parent_keys": list(raw.keys())
                })
            return [normalize_item(r, requested_index) for r in rows]

        # Another possibility: {"data":[{...}, {...}]}
        for candidate in ("data", "result", "results", "records", "items"):
            if candidate in raw and isinstance(raw[candidate], list):
                return [normalize_item(r, requested_index) for r in raw[candidate] if isinstance(r, dict)]

        # Fallback: if dict itself looks like one record
        return [normalize_item(raw, requested_index)]

    raise ValueError(f"Unsupported GFZ response format: {type(raw)}")


def normalize_item(item: Dict[str, Any], requested_index: str) -> Dict[str, Any]:
    """
    Extract best-effort source time/value/status from one API item.
    """
    # time candidates
    source_time_str = None
    for key in ("time", "Time", "datetime", "date", "timestamp"):
        if key in item:
            source_time_str = item.get(key)
            break

    # value candidates
    value = None
    if requested_index in item:
        value = item.get(requested_index)
    elif requested_index.lower() in [str(k).lower() for k in item.keys()]:
        for k in item.keys():
            if str(k).lower() == requested_index.lower():
                value = item.get(k)
                break
    elif "value" in item:
        value = item.get("value")

    status = item.get("status") if isinstance(item, dict) else None

    source_time_dt = parse_iso_utc(source_time_str) if isinstance(source_time_str, str) else None

    # Dedupe key: stable key from source time + index + status + value
    dedupe_key = f"{requested_index}|{source_time_str}|{status}|{value}"

    return {
        "dedupe_key": dedupe_key,
        "index_name": requested_index,
        "source_time_raw": source_time_str,
        "source_time_utc": source_time_dt,
        "value": value,
        "status": status,
        "source_payload": item,   # keep raw normalized item for debugging
        "received_at_utc": utc_now(),  # same style/zone as sensor app
        "ingestion": {
            "source": "gfz_kp_api",
            "version": 1
        }
    }


def upsert_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        logger.info("No rows to upsert.")
        return

    ops = []
    for row in rows:
        ops.append(
            UpdateOne(
                {"dedupe_key": row["dedupe_key"]},
                {
                    "$setOnInsert": row,
                    "$set": {
                        "last_seen_at_utc": utc_now()
                    }
                },
                upsert=True
            )
        )

    result = collection.bulk_write(ops, ordered=False)
    logger.info(
        "Upserted GFZ Kp rows: matched=%d modified=%d upserted=%d",
        result.matched_count,
        result.modified_count,
        len(result.upserted_ids) if result.upserted_ids else 0
    )


def run_once():
    end_dt = utc_now()
    start_dt = end_dt - timedelta(hours=LOOKBACK_HOURS)

    raw = fetch_kp_data(
        index_name=KP_INDEX,
        start_dt=start_dt,
        end_dt=end_dt,
        only_definitive=ONLY_DEFINITIVE
    )

    logger.info("GFZ API response type: %s", type(raw).__name__)
    rows = normalize_gfz_response(raw, KP_INDEX)
    logger.info("Normalized rows count: %d", len(rows))

    upsert_rows(rows)


def main():
    # Startup connectivity check
    try:
        mongo_client.admin.command("ping")
        logger.info("Connected to MongoDB successfully.")
    except Exception as e:
        logger.exception("Failed to connect to MongoDB: %s", e)
        raise

    while True:
        try:
            run_once()
        except requests.RequestException as e:
            logger.exception("HTTP/API error while fetching GFZ Kp data: %s", e)
        except PyMongoError as e:
            logger.exception("MongoDB error while storing GFZ Kp data: %s", e)
        except Exception as e:
            logger.exception("Unexpected error in Kp ingestor loop: %s", e)

        logger.info("Sleeping for %d seconds before next poll...", POLL_INTERVAL_SECONDS)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()