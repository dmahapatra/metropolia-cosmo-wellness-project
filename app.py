import ssl
import json
import time
import uuid
import logging
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# ============================================================
# MQTT broker details
# ============================================================
MQTT_HOST = "aiot-garage.cloud.shiftr.io"
MQTT_PORT = 1883
MQTT_USERNAME = "aiot-garage"
MQTT_PASSWORD = "xbk4O60zdExseMOa"
MQTT_TOPIC = "sensors/max30102"
MQTT_KEEPALIVE = 60

# ============================================================
# MongoDB details
# ============================================================
MONGO_URI = "mongodb+srv://debojyotimahapatra_db_user:NM2K3z8HGe3qWJjZ@cluster0.7ejimre.mongodb.net/?appName=Cluster0"
MONGO_DB_NAME = "cosmo_project"
MONGO_COLLECTION_NAME = "sensor_data"

# If True: store each IBI sample as separate document
# If False: store the whole message as a single document
EXPLODE_IBI_SAMPLES = False

LOG_LEVEL = "INFO"

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

# Create helpful indexes (safe to call repeatedly)
try:
    collection.create_index("received_at_utc")
    collection.create_index("topic")
    collection.create_index("batch_id")
    collection.create_index("time")  # producer timestamp
except PyMongoError as e:
    logger.warning("Could not create indexes: %s", e)


def utc_now():
    return datetime.now(timezone.utc)


def parse_sensor_payload(payload_bytes):
    """
    Parse and validate sensor payload.

    Expected format (from sensor):
    {
      "time": 1771946976,
      "hr": 73.4,
      "rmssd": 78.7,
      "spo2": 99.4,
      "beats": 31,
      "ibi": [862, 782, 791, ...]
    }
    """
    try:
        payload_str = payload_bytes.decode("utf-8")
        data = json.loads(payload_str)
    except Exception as e:
        raise ValueError(f"Invalid JSON payload: {e}")

    if not isinstance(data, dict):
        raise ValueError("Payload must be a JSON object")

    if "time" not in data:
        raise ValueError("Missing 'time' field in payload")

    # Basic type coercion + validation
    parsed = {
        "time": int(data["time"]),
        "hr": float(data["hr"]) if data.get("hr") is not None else None,
        "rmssd": float(data["rmssd"]) if data.get("rmssd") is not None else None,
        "spo2": float(data["spo2"]) if data.get("spo2") is not None else None,
        "beats": int(data["beats"]) if data.get("beats") is not None else None,
        "ibi": data.get("ibi", []),
        "payload_keys": list(data.keys())
    }

    if parsed["ibi"] is None:
        parsed["ibi"] = []

    if not isinstance(parsed["ibi"], list):
        raise ValueError("'ibi' must be a list")

    cleaned_ibi = []
    for i, v in enumerate(parsed["ibi"]):
        if isinstance(v, (int, float)):
            cleaned_ibi.append(int(v))
        else:
            logger.warning("Skipping non-numeric ibi[%d]=%r", i, v)

    parsed["ibi"] = cleaned_ibi
    parsed["ibi_count"] = len(cleaned_ibi)

    return parsed


def save_batch_document(topic, parsed):
    """
    Save one MongoDB document per MQTT message.
    """
    batch_id = str(uuid.uuid4())

    doc = {
        "batch_id": batch_id,
        "topic": topic,
        "received_at_utc": utc_now(),

        # original device timestamp + derived UTC datetime
        "time": parsed["time"],
        "ts_utc": datetime.fromtimestamp(parsed["time"], tz=timezone.utc),

        "metrics": {
            "hr": parsed.get("hr"),
            "rmssd": parsed.get("rmssd"),
            "spo2": parsed.get("spo2"),
            "beats": parsed.get("beats"),
            "ibi": parsed.get("ibi"),
            "ibi_count": parsed.get("ibi_count"),
        },

        "payload_keys": parsed.get("payload_keys"),
        "ingestion": {
            "source": "mqtt",
            "version": 2
        }
    }

    result = collection.insert_one(doc)
    logger.info(
        "Saved batch document: _id=%s batch_id=%s time=%s hr=%s spo2=%s ibi_count=%s",
        result.inserted_id,
        batch_id,
        parsed["time"],
        parsed.get("hr"),
        parsed.get("spo2"),
        parsed.get("ibi_count"),
    )


def save_exploded_ibi_documents(topic, parsed):
    """
    Save one MongoDB document per IBI sample.
    Optional mode for time-series querying on each IBI value.
    """
    batch_id = str(uuid.uuid4())
    now = utc_now()

    docs = []
    for idx, value in enumerate(parsed["ibi"]):
        docs.append({
            "batch_id": batch_id,
            "topic": topic,
            "received_at_utc": now,
            "time": parsed["time"],
            "ts_utc": datetime.fromtimestamp(parsed["time"], tz=timezone.utc),

            "sample_index": idx,
            "ibi_value": int(value),

            "metrics": {
                "hr": parsed.get("hr"),
                "rmssd": parsed.get("rmssd"),
                "spo2": parsed.get("spo2"),
                "beats": parsed.get("beats"),
            },

            "ibi_count_in_batch": parsed.get("ibi_count"),
            "payload_keys": parsed.get("payload_keys"),

            "ingestion": {
                "source": "mqtt",
                "version": 2
            }
        })

    if docs:
        result = collection.insert_many(docs, ordered=False)
        logger.info("Saved %d exploded IBI docs for batch_id=%s", len(result.inserted_ids), batch_id)
    else:
        logger.info("No IBI samples to explode; saving batch doc instead.")
        save_batch_document(topic, parsed)


# ============================================================
# MQTT callbacks (VERSION2 signatures)
# ============================================================
def on_connect(client, userdata, flags, reason_code, properties):
    if getattr(reason_code, "value", reason_code) == 0:
        logger.info("Connected to MQTT broker %s:%d", MQTT_HOST, MQTT_PORT)
        client.subscribe(MQTT_TOPIC, qos=0)
        logger.info("Subscribed to topic: %s", MQTT_TOPIC)
    else:
        logger.error("MQTT connect failed with reason_code=%s", reason_code)


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    if getattr(reason_code, "value", reason_code) != 0:
        logger.warning("Unexpected MQTT disconnect (reason_code=%s). Auto-reconnect will retry.", reason_code)
    else:
        logger.info("MQTT disconnected cleanly.")


def on_message(client, userdata, msg):
    logger.info("Message received on topic=%s payload_size=%d", msg.topic, len(msg.payload))

    try:
        parsed = parse_sensor_payload(msg.payload)

        if EXPLODE_IBI_SAMPLES:
            save_exploded_ibi_documents(msg.topic, parsed)
        else:
            save_batch_document(msg.topic, parsed)

    except ValueError as ve:
        logger.error("Payload validation error: %s", ve)
    except PyMongoError as me:
        logger.exception("MongoDB error while saving data: %s", me)
    except Exception as e:
        logger.exception("Unexpected processing error: %s", e)


def build_mqtt_client():
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"max30102-ingestor-{uuid.uuid4().hex[:8]}",
        protocol=mqtt.MQTTv311
    )
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # TLS (as requested)
    client.tls_set(
        ca_certs=None,
        certfile=None,
        keyfile=None,
        cert_reqs=ssl.CERT_NONE,
        tls_version=ssl.PROTOCOL_TLS_CLIENT
    )
    client.tls_insecure_set(True)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def main():
    # Check Mongo connectivity at startup
    try:
        mongo_client.admin.command("ping")
        logger.info("Connected to MongoDB successfully.")
    except Exception as e:
        logger.exception("Failed to connect to MongoDB: %s", e)
        raise

    client = build_mqtt_client()

    while True:
        try:
            logger.info("Connecting to MQTT broker...")
            client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
            client.loop_forever(retry_first_connection=True)
        except KeyboardInterrupt:
            logger.info("Stopping on keyboard interrupt.")
            try:
                client.disconnect()
            except Exception:
                pass
            break
        except Exception as e:
            logger.exception("MQTT loop crashed. Retrying in 5 seconds: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()