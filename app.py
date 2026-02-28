# app.py
import os
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
# MQTT (HiveMQ Cloud) - read from env first (recommended for Heroku)
# ============================================================
MQTT_HOST = os.getenv("MQTT_HOST", "xxxx")
MQTT_PORT = int(os.getenv("MQTT_PORT", "xxxx"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "xxxx")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "xxxxx")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "xxxxx")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "xxxxxn")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "xx"))

# ============================================================
# MongoDB - read from env first (recommended for Heroku)
# ============================================================
MONGO_URI = os.getenv(
    "MONGO_URI",
    "xxxxx",
)
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "cosmo_project")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "sensor_data_latest")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================
# MongoDB setup
# ============================================================
mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
db = mongo_client[MONGO_DB_NAME]
collection = db[MONGO_COLLECTION_NAME]

# Helpful indexes (safe to call repeatedly)
try:
    collection.create_index("received_at_utc")
    collection.create_index("topic")
    collection.create_index("message_id")
    collection.create_index("sensor_time_epoch")
except PyMongoError as e:
    logger.warning("Could not create indexes: %s", e)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_sensor_payload(payload_bytes: bytes) -> dict:
    """
    Expected payload from sensor:
    {"time":1771946976,"hr":73.4,"rmssd":78.7,"spo2":99.4,"beats":31,"ibi":[862,782,...]}
    """
    try:
        payload_str = payload_bytes.decode("utf-8", errors="strict")
        data = json.loads(payload_str)
    except Exception as e:
        raise ValueError(f"Invalid JSON payload: {e}")

    if not isinstance(data, dict):
        raise ValueError("Payload must be a JSON object")

    required = ["time", "hr", "rmssd", "spo2", "beats", "ibi"]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Missing required field(s): {missing}")

    # Validate types + clean ibi
    t = data["time"]
    if not isinstance(t, (int, float)):
        raise ValueError("'time' must be a number (epoch seconds)")

    hr = data["hr"]
    rmssd = data["rmssd"]
    spo2 = data["spo2"]
    beats = data["beats"]
    ibi = data["ibi"]

    if not isinstance(hr, (int, float)):
        raise ValueError("'hr' must be a number")
    if not isinstance(rmssd, (int, float)):
        raise ValueError("'rmssd' must be a number")
    if not isinstance(spo2, (int, float)):
        raise ValueError("'spo2' must be a number")
    if not isinstance(beats, int):
        raise ValueError("'beats' must be an integer")
    if not isinstance(ibi, list):
        raise ValueError("'ibi' must be a list")

    cleaned_ibi = []
    for i, v in enumerate(ibi):
        if isinstance(v, (int, float)):
            cleaned_ibi.append(float(v))
        else:
            logger.warning("Skipping non-numeric ibi[%d]=%r", i, v)

    if len(cleaned_ibi) == 0:
        raise ValueError("No valid numeric values in 'ibi' array")

    sensor_time_epoch = int(t)
    sensor_time_utc = datetime.fromtimestamp(sensor_time_epoch, tz=timezone.utc)

    return {
        "sensor_time_epoch": sensor_time_epoch,
        "sensor_time_utc": sensor_time_utc,
        "hr": float(hr),
        "rmssd": float(rmssd),
        "spo2": float(spo2),
        "beats": int(beats),
        "ibi": cleaned_ibi,
        "ibi_count": len(cleaned_ibi),
        "payload_keys": list(data.keys()),
        "raw": data,
    }


def save_document(topic: str, parsed: dict) -> None:
    message_id = str(uuid.uuid4())

    doc = {
        "message_id": message_id,
        "topic": topic,
        "received_at_utc": utc_now(),
        "sensor_time_epoch": parsed["sensor_time_epoch"],
        "sensor_time_utc": parsed["sensor_time_utc"],
        "metrics": {
            "hr": parsed["hr"],
            "rmssd": parsed["rmssd"],
            "spo2": parsed["spo2"],
            "beats": parsed["beats"],
        },
        "ibi": parsed["ibi"],
        "ibi_count": parsed["ibi_count"],
        "payload_keys": parsed.get("payload_keys"),
        "ingestion": {
            "source": "mqtt",
            "broker": "hivemq_cloud",
            "version": 1,
        },
    }

    result = collection.insert_one(doc)
    logger.info(
        "Saved sensor doc: _id=%s message_id=%s hr=%.2f spo2=%.2f rmssd=%.2f ibi_count=%d",
        result.inserted_id,
        message_id,
        parsed["hr"],
        parsed["spo2"],
        parsed["rmssd"],
        parsed["ibi_count"],
    )


# ============================================================
# MQTT callbacks (use Callback API v1 to avoid signature mismatch)
# ============================================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT broker %s:%d", MQTT_HOST, MQTT_PORT)
        client.subscribe(MQTT_TOPIC, qos=0)
        logger.info("Subscribed to topic: %s", MQTT_TOPIC)
    else:
        logger.error("MQTT connect failed with rc=%s", rc)


def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning("Unexpected MQTT disconnect (rc=%s). Auto-reconnect will retry.", rc)
    else:
        logger.info("MQTT disconnected cleanly.")


def on_message(client, userdata, msg):
    logger.info("Message received topic=%s payload_size=%d", msg.topic, len(msg.payload))

    try:
        parsed = parse_sensor_payload(msg.payload)
        save_document(msg.topic, parsed)
    except ValueError as ve:
        logger.error("Payload validation error: %s", ve)
    except PyMongoError as me:
        logger.exception("MongoDB error while saving data: %s", me)
    except Exception as e:
        logger.exception("Unexpected processing error: %s", e)


def build_mqtt_client() -> mqtt.Client:
    # IMPORTANT: force Callback API v1 so callback signatures match (no ReasonCodes / extra args issues)
    client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv311,
        transport="tcp",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
    )

    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # TLS for HiveMQ Cloud (8883)
    # You requested CERT_NONE (no server cert verification). Works, but not recommended for production.
    client.tls_set(
        cert_reqs=ssl.CERT_NONE,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )
    client.tls_insecure_set(True)

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