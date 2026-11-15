import json
import os
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "f1-telemetry-live")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print(f"Mendengarkan pesan dari topik '{KAFKA_TOPIC}' di {KAFKA_BOOTSTRAP_SERVERS}...")
for message in consumer:
    data = message.value
    print(
        f"[TERIMA] Key={message.key.decode('utf-8')} | "
        f"Lap {data['lap_number']} | "
        f"Speed={data['telemetry']['speed']} km/h | "
        f"Throttle={data['telemetry']['throttle']}% | "
        f"Brake={data['telemetry']['brake']}"
    )