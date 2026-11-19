import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

import fastf1
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("F1-Producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "f1-telemetry-live")
KAFKA_CONTROL_TOPIC = os.getenv("KAFKA_CONTROL_TOPIC", "f1-stream-control")
STREAM_DELAY = float(os.getenv("STREAM_DELAY", "0.3"))

os.makedirs("./cache", exist_ok=True)
fastf1.Cache.enable_cache("./cache")

# State konfigurasi aktif (di-update otomatis oleh listener Kafka control topic)
active_config = {
    "filtered_year": int(os.getenv("F1_YEAR", "2024")),
    "grand_prix": os.getenv("F1_GP", "Monza"),
    "session": os.getenv("F1_SESSION", "R"),
    "drivers": None,  # None = ambil semua pembalap; atau list seperti ["VER", "LEC"]
    "max_laps": int(os.getenv("MAX_LAPS", "5")),
}
config_lock = threading.Lock()
reload_event = threading.Event()


def listen_stream_control():
    """Thread latar belakang untuk menangkap perubahan filter dari FastAPI/Frontend."""
    consumer = KafkaConsumer(
        KAFKA_CONTROL_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id="f1-producer-control-group",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    logger.info(f"Mendengarkan perintah filter dinamis di topik '{KAFKA_CONTROL_TOPIC}'...")

    for msg in consumer:
        new_cfg = msg.value
        if not isinstance(new_cfg, dict):
            continue
        with config_lock:
            active_config["filtered_year"] = int(new_cfg.get("filtered_year", active_config["filtered_year"]))
            active_config["grand_prix"] = str(new_cfg.get("grand_prix", active_config["grand_prix"]))
            active_config["session"] = str(new_cfg.get("session", active_config["session"])).upper()
            raw_drivers = new_cfg.get("drivers")
            active_config["drivers"] = [d.strip().upper() for d in raw_drivers] if raw_drivers else None
            active_config["max_laps"] = int(new_cfg.get("max_laps") or 5)

        logger.info(f"Filter baru diterima dari Frontend: {active_config}. Memuat ulang sesi...")
        reload_event.set()


def sanitize_value(val):
    if pd.isna(val):
        return None
    return val.item() if hasattr(val, "item") else val


def extract_race_telemetry(year: int, grand_prix: str, session_type: str, drivers: list[str] | None, max_laps: int):
    logger.info(f"Memuat sesi FastF1: {year} {grand_prix} ({session_type})...")
    session = fastf1.get_session(year, grand_prix, session_type)
    session.load(telemetry=True, laps=True, weather=False, messages=False)

    # Jika drivers=None/kosong, ambil seluruh pembalap yang turun pada sesi tersebut
    if not drivers:
        target_drivers = [
            session.get_driver(drv_num)["Abbreviation"]
            for drv_num in session.drivers
            if session.get_driver(drv_num)["Abbreviation"]
        ]
    else:
        target_drivers = drivers

    all_frames = []
    for drv in target_drivers:
        drv_laps = session.laps.pick_drivers(drv)
        if drv_laps.empty:
            continue

        drv_laps = drv_laps[drv_laps["LapNumber"] <= max_laps]
        for _, lap in drv_laps.iterrows():
            try:
                tel = lap.get_telemetry()
                if tel.empty:
                    continue

                # Resolusi iloc[::2] (~0.5s) agar grafik dashboard halus dan tidak patah-patah
                tel_sampled = tel.iloc[::2].copy()
                tel_sampled["Driver"] = drv
                tel_sampled["Team"] = sanitize_value(lap["Team"])
                tel_sampled["LapNumber"] = sanitize_value(lap["LapNumber"])
                tel_sampled["Compound"] = sanitize_value(lap["Compound"])
                tel_sampled["TyreLife"] = sanitize_value(lap["TyreLife"])
                tel_sampled["SessionTimeSec"] = tel_sampled["SessionTime"].dt.total_seconds()
                all_frames.append(tel_sampled)
            except Exception:
                continue

    if not all_frames:
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    return combined.sort_values("SessionTimeSec").reset_index(drop=True)


def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    control_thread = threading.Thread(target=listen_stream_control, daemon=True)
    control_thread.start()

    while True:
        reload_event.clear()
        with config_lock:
            cfg = dict(active_config)

        df = extract_race_telemetry(
            year=cfg["filtered_year"],
            grand_prix=cfg["grand_prix"],
            session_type=cfg["session"],
            drivers=cfg["drivers"],
            max_laps=cfg["max_laps"],
        )

        if df.empty:
            logger.warning("Data telemetri kosong untuk filter ini. Menunggu filter baru (5 detik)...")
            reload_event.wait(timeout=5)
            continue

        logger.info(f"Streaming {len(df)} titik data ke '{KAFKA_TOPIC}'...")
        for _, row in df.iterrows():
            # Hentikan loop balapan lama seketika jika user mengganti filter di UI
            if reload_event.is_set():
                logger.info("Menghentikan stream lama untuk beralih ke filter baru...")
                break

            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "race_info": {
                    "year": cfg["filtered_year"],
                    "grand_prix": cfg["grand_prix"],
                    "session": cfg["session"],
                },
                "driver": sanitize_value(row["Driver"]),
                "team": sanitize_value(row["Team"]),
                "lap_number": sanitize_value(row["LapNumber"]),
                "compound": sanitize_value(row["Compound"]),
                "tyre_life": sanitize_value(row["TyreLife"]),
                "session_time_sec": sanitize_value(row["SessionTimeSec"]),
                "telemetry": {
                    "speed": sanitize_value(row.get("Speed")),
                    "rpm": sanitize_value(row.get("RPM")),
                    "gear": sanitize_value(row.get("nGear")),
                    "throttle": sanitize_value(row.get("Throttle")),
                    "brake": sanitize_value(row.get("Brake")),
                    "drs": sanitize_value(row.get("DRS")),
                    "distance": sanitize_value(row.get("Distance")),
                    "x": sanitize_value(row.get("X")),
                    "y": sanitize_value(row.get("Y")),
                    "z": sanitize_value(row.get("Z")),
                },
            }
            producer.send(KAFKA_TOPIC, key=payload["driver"], value=payload)
            time.sleep(STREAM_DELAY)


if __name__ == "__main__":
    run_producer()