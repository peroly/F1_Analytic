import json
import logging
import math
import os
import time
from datetime import datetime, timezone

import fastf1
import numpy as np
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Konfigurasi Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("F1-Kafka-Producer")

# Konfigurasi Environment 
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "f1-telemetry-live")
CACHE_DIR = os.getenv("FASTF1_CACHE_DIR", "./cache")

# Parameter Balapan F1 yang Disimulasikan
F1_YEAR = int(os.getenv("F1_YEAR", "2024"))
F1_GP = os.getenv("F1_GP", "Monza")          
F1_SESSION = os.getenv("F1_SESSION", "R")    
DRIVERS = os.getenv("F1_DRIVERS", "VER,LEC").split(",")  
MAX_LAPS = int(os.getenv("F1_MAX_LAPS", "5"))  
STREAM_DELAY = float(os.getenv("STREAM_DELAY", "0.2"))  # Jeda antar data (detik)


def create_kafka_producer(servers: str, retries: int = 10, delay: int = 3) -> KafkaProducer:
    """Membuat koneksi KafkaProducer dengan mekanisme retry jika container Kafka baru menyala."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            logger.info(f"Berhasil terhubung ke Kafka Broker di {servers}")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka belum siap (percobaan {attempt}/{retries}). Menunggu {delay} detik..."
            )
            time.sleep(delay)
    raise RuntimeError(f"Gagal terhubung ke Kafka di {servers} setelah {retries} percobaan.")


def sanitize_value(val):
    """Mengonversi tipe data NumPy/Pandas serta menangani NaN/NaT agar 100% valid JSON."""
    if val is None or (isinstance(val, float) and math.isnan(val)) or pd.isna(val):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return round(float(val), 4)
    if isinstance(val, (np.bool_, bool)):
        return bool(val)
    if isinstance(val, pd.Timedelta):
        return round(val.total_seconds(), 4)
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.isoformat()
    return val


def extract_race_telemetry(year: int, gp: str, session_type: str, drivers: list[str], max_laps: int) -> pd.DataFrame:
    """Mengunduh dan menyiapkan dataframe telemetri kronologis dari FastF1."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    fastf1.Cache.enable_cache(CACHE_DIR)

    logger.info(f"Memuat sesi FastF1: {year} {gp} ({session_type})...")
    session = fastf1.get_session(year, gp, session_type)
    session.load(telemetry=True, laps=True, weather=False)

    all_records = []

    for drv in drivers:
        drv = drv.strip().upper()
        logger.info(f"Mengekstrak data telemetri pembalap: {drv}")

        # Kompatibilitas versi fastf1
        if hasattr(session.laps, "pick_drivers"):
            drv_laps = session.laps.pick_drivers(drv)
        else:
            drv_laps = session.laps.pick_driver(drv)

        if drv_laps.empty:
            logger.warning(f"Data lap tidak ditemukan untuk pembalap {drv}. Melewati...")
            continue

        if max_laps > 0:
            drv_laps = drv_laps[drv_laps["LapNumber"] <= max_laps]

        for _, lap in drv_laps.iterlaps():
            try:
                tel = lap.get_telemetry()
            except Exception as e:
                logger.warning(f"Gagal mengambil telemetri {drv} Lap {lap['LapNumber']}: {e}")
                continue

            if tel.empty:
                continue

            # Ambil sampel setiap 5 baris 
            tel_sampled = tel.iloc[::5].copy()

            tel_sampled["driver"] = drv
            tel_sampled["team"] = lap.get("Team", "Unknown")
            tel_sampled["lap_number"] = lap.get("LapNumber", 0)
            tel_sampled["compound"] = lap.get("Compound", "UNKNOWN")
            tel_sampled["tyre_life"] = lap.get("TyreLife", 0)
            tel_sampled["session_time_sec"] = tel_sampled["SessionTime"].dt.total_seconds()

            all_records.append(tel_sampled)

    if not all_records:
        raise ValueError("Tidak ada data telemetri yang berhasil diekstrak.")

    combined_df = pd.concat(all_records, ignore_index=True)
    # Mengrutkan berdasarkan waktu sesi agar data kedua pembalap mengalir beriringan secara real-time
    combined_df.sort_values(by="session_time_sec", inplace=True)
    combined_df.reset_index(drop=True, inplace=True)

    logger.info(f"Total {len(combined_df)} titik data telemetri siap di-stream.")
    return combined_df


def run_producer():
    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    df = extract_race_telemetry(F1_YEAR, F1_GP, F1_SESSION, DRIVERS, MAX_LAPS)

    logger.info(f"Mulai menembakkan data ke topik Kafka '{KAFKA_TOPIC}' (delay: {STREAM_DELAY}s)...")
    sent_count = 0

    try:
        for _, row in df.iterrows():
            payload = {
                # Timestamp UTC saat data dikirim (untuk timeField MongoDB Time-Series)
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "race_info": {
                    "year": F1_YEAR,
                    "grand_prix": F1_GP,
                    "session": F1_SESSION,
                },
                "driver": sanitize_value(row.get("driver")),
                "team": sanitize_value(row.get("team")),
                "lap_number": sanitize_value(row.get("lap_number")),
                "compound": sanitize_value(row.get("compound")),
                "tyre_life": sanitize_value(row.get("tyre_life")),
                "session_time_sec": sanitize_value(row.get("session_time_sec")),
                "telemetry": {
                    "speed": sanitize_value(row.get("Speed")),         # km/h
                    "rpm": sanitize_value(row.get("RPM")),             # Engine RPM
                    "gear": sanitize_value(row.get("nGear")),          # 1 - 8
                    "throttle": sanitize_value(row.get("Throttle")),   # 0 - 100 %
                    "brake": sanitize_value(row.get("Brake")),         # True / False
                    "drs": sanitize_value(row.get("DRS")),             # Status DRS
                    "distance": sanitize_value(row.get("Distance")),   # Meter dari garis start
                    "x": sanitize_value(row.get("X")),                 # Koordinat sirkuit X
                    "y": sanitize_value(row.get("Y")),                 # Koordinat sirkuit Y
                    "z": sanitize_value(row.get("Z")),                 # Koordinat sirkuit Z
                },
            }

            # Gunakan kode pembalap sebagai Kafka Message Key
            producer.send(KAFKA_TOPIC, key=payload["driver"], value=payload)
            sent_count += 1

            if sent_count % 25 == 0:
                producer.flush()
                logger.info(
                    f"[{sent_count}/{len(df)}] Terkirim -> Driver: {payload['driver']} | "
                    f"Lap: {payload['lap_number']} | Speed: {payload['telemetry']['speed']} km/h | "
                    f"Gear: {payload['telemetry']['gear']}"
                )

            time.sleep(STREAM_DELAY)

    except KeyboardInterrupt:
        logger.info("Streaming dihentikan secara manual oleh pengguna (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Producer selesai. Total pesan terkirim: {sent_count}")


if __name__ == "__main__":
    run_producer()