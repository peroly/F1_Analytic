import json
import logging
import os
import sys
import time
from pathlib import Path

import kafka.errors as kafka_errors
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import PyMongoError

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from cleaner import clean_telemetry_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("F1-ETL-Consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "f1-telemetry-live")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "f1-etl-mongo-group")
AUTO_OFFSET_RESET = os.getenv("KAFKA_OFFSET_RESET", "latest")

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://f1_app_user:f1banjutjaya@localhost:27017/f1_analytics?authSource=f1_analytics",
)
MONGO_DB_NAME = os.getenv("MONGO_DB", "f1_analytics")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "telemetry_live")


def create_kafka_consumer(servers: str, topic: str, retries: int = 10, delay: int = 3) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=servers.split(","),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset=AUTO_OFFSET_RESET,
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            logger.info(f"Terhubung ke Kafka Broker ({servers}) | Topic: '{topic}'")
            return consumer
        except (kafka_errors.KafkaError, Exception) as e:
            logger.warning(
                f"Kafka belum siap ({e}) - percobaan {attempt}/{retries}. Menunggu {delay} detik..."
            )
            time.sleep(delay)
    raise RuntimeError(f"Gagal terhubung ke Kafka di {servers} setelah {retries} percobaan.")


def connect_mongodb():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    logger.info(f"Terhubung ke MongoDB -> {MONGO_DB_NAME}.{MONGO_COLLECTION_NAME}")
    return client, collection

def run_etl_consumer():
    mongo_client, collection = connect_mongodb()
    consumer = create_kafka_consumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    logger.info("Menunggu aliran data telemetri dari Kafka (Tekan Ctrl+C untuk berhenti)...")
    processed_count = 0

    try:
        for message in consumer:
            raw_payload = message.value
            cleaned_doc = clean_telemetry_data(raw_payload)

            if cleaned_doc is None:
                logger.warning("Pesan dilewati karena format payload tidak valid.")
                continue

            try:
                collection.insert_one(cleaned_doc)
                processed_count += 1

                if processed_count % 25 == 0:
                    meta = cleaned_doc["metadata"]
                    logger.info(
                        f"[{processed_count} tersimpan] Session: {meta['session_id']} | "
                        f"Driver: {meta['driver_code']} | Lap: {cleaned_doc['lap_number']} | "
                        f"Speed: {cleaned_doc['speed']} km/h"
                    )
            except PyMongoError as db_err:
                logger.error(f"Gagal menyimpan dokumen ke MongoDB: {db_err}")

    except KeyboardInterrupt:
        logger.info("ETL Consumer dihentikan secara manual oleh pengguna (Ctrl+C).")
    finally:
        consumer.close()
        mongo_client.close()
        logger.info(f"ETL Consumer selesai. Total dokumen tersimpan ke MongoDB: {processed_count}")

if __name__ == "__main__":
    run_etl_consumer()