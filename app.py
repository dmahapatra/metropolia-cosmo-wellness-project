import json
import time
import uuid
import logging
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# MQTT broker details
#MQTT_HOST = "aiot-garage.cloud.shiftr.io"
#MQTT_PORT = 1883
#MQTT_USERNAME = "aiot-garage"
#MQTT_PASSWORD = "xbk4O60zdExseMOa"
#MQTT_TOPIC = "sensors/max30102"
#MQTT_KEEPALIVE = 60

MQTT_HOST = "d87aad0df47f4a46953a7b9c93c6aada.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USERNAME = "Cosmo"
MQTT_PASSWORD = "jO^HuAm4AzFLPS"
MQTT_TOPIC = "sensors/max30102"
MQTT_CLIENT_ID = "max30102-monitor-python"
MQTT_KEEPALIVE = 60


# MongoDB details
MONGO_URI = "mongodb+srv://debojyotimahapatra_db_user:NM2K3z8HGe3qWJjZ@cluster0.7ejimre.mongodb.net/?appName=Cluster0"
MONGO_DB_NAME = "cosmo_project"
MONGO_COLLECTION_NAME = "sensor_data_latest"   # your requested collection name (with space)

EXPLODE_RED_SAMPLES = False

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
except PyMongoError as e:
    logger.warning("Could not create indexes: %s", e)


def utc_now():
    return datetime.now(timezone.utc)


def parse_sensor_payload(payload_bytes):
    """
    Parse and validate sensor payload.
    Expected format:
    {
      "t0_ms": 21866,
      "temp_c": 27.25,
      "ir": [...],
      "red": [...]
    }
    """
    try:
        payload_str = payload_bytes.decode("utf-8")
        data = json.loads(payload_str)
    except Exception as e:
        raise ValueError(f"Invalid JSON payload: {e}")

    if not isinstance(data, dict):
        raise ValueError("Payload must be a JSON object")

    red = data.get("red")
    if red is None:
        raise ValueError("Missing 'red' field in payload")
    if not isinstance(red, list):
        raise ValueError("'red' must be a list")

    cleaned_red = []
    for i, v in enumerate(red):
        if isinstance(v, (int, float)):
            cleaned_red.append(v)
        else:
            logger.warning("Skipping non-numeric red[%d]=%r", i, v)

    if len(cleaned_red) == 0:
        raise ValueError("No valid numeric values in 'red' array")

    parsed = {
        "t0_ms": data.get("t0_ms"),
        "temp_c": data.get("temp_c"),
        "red": cleaned_red,
        "red_count": len(cleaned_red),
        "ir_count": len(data.get("ir", [])) if isinstance(data.get("ir"), list) else None,
        "payload_keys": list(data.keys())
    }
    return parsed


def save_batch_document(topic, parsed):
    """
    Save one MongoDB document per MQTT batch.
    """
    batch_id = str(uuid.uuid4())

    doc = {
        "batch_id": batch_id,
        "topic": topic,
        "received_at_utc": utc_now(),
        "sensor": {
            "type": "MAX30102",
            "t0_ms": parsed.get("t0_ms"),
            "temp_c": parsed.get("temp_c"),
        },
        "red": parsed["red"],             
        "red_count": parsed["red_count"],
        "ir_count": parsed.get("ir_count"), 
        "payload_keys": parsed.get("payload_keys"),
        "ingestion": {
            "source": "mqtt",
            "version": 1
        }
    }

    result = collection.insert_one(doc)
    logger.info(
        "Saved batch document: _id=%s batch_id=%s red_count=%d",
        result.inserted_id, batch_id, parsed["red_count"]
    )


def save_exploded_red_documents(topic, parsed):
    """
    Save one MongoDB document per red sample.
    Optional mode for time-series querying.
    """
    batch_id = str(uuid.uuid4())
    now = utc_now()
    docs = []

    for idx, value in enumerate(parsed["red"]):
        docs.append({
            "batch_id": batch_id,
            "topic": topic,
            "received_at_utc": now,
            "sample_index": idx,
            "red_value": value,
            "sensor": {
                "type": "MAX30102",
                "t0_ms": parsed.get("t0_ms"),
                "temp_c": parsed.get("temp_c"),
            },
            "red_count_in_batch": parsed["red_count"],
            "ingestion": {
                "source": "mqtt",
                "version": 1
            }
        })

    if docs:
        result = collection.insert_many(docs, ordered=False)
        logger.info("Saved %d exploded RED docs for batch_id=%s", len(result.inserted_ids), batch_id)


# ============================================================
# MQTT callbacks
# ============================================================
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("Connected to MQTT broker %s:%d", MQTT_HOST, MQTT_PORT)
        client.subscribe(MQTT_TOPIC, qos=0)
        logger.info("Subscribed to topic: %s", MQTT_TOPIC)
    else:
        logger.error("MQTT connect failed with rc=%s", rc)


def on_disconnect(client, userdata, rc, properties=None):
    if rc != 0:
        logger.warning("Unexpected MQTT disconnect (rc=%s). Auto-reconnect will retry.", rc)
    else:
        logger.info("MQTT disconnected cleanly.")


def on_message(client, userdata, msg):
    logger.info("Message received on topic=%s payload_size=%d", msg.topic, len(msg.payload))

    try:
        parsed = parse_sensor_payload(msg.payload)

        if EXPLODE_RED_SAMPLES:
            save_exploded_red_documents(msg.topic, parsed)
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
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"max30102-ingestor-{uuid.uuid4().hex[:8]}"
    )
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # Auto reconnect backoff
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