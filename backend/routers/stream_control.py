import json
import os
from typing import Literal, Optional

import fastf1
from fastapi import APIRouter, HTTPException, Query
from kafka import KafkaProducer
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/v1", tags=["Dynamic Filters & Stream Control"])

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_CONTROL_TOPIC = os.getenv("KAFKA_CONTROL_TOPIC", "f1-stream-control")

os.makedirs("../cache", exist_ok=True)
fastf1.Cache.enable_cache("../cache")

_control_producer: Optional[KafkaProducer] = None


def get_control_producer() -> KafkaProducer:
    global _control_producer
    if _control_producer is None:
        _control_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _control_producer


class StreamFilterRequest(BaseModel):
    filtered_year: int = Field(..., ge=2018, le=2030)
    grand_prix: str = Field(..., min_length=2)
    session: Literal["FP1", "FP2", "FP3", "Q", "SQ", "S", "R"]
    drivers: Optional[list[str]] = Field(default=None)
    max_laps: Optional[int] = Field(default=5, ge=1, le=78)


@router.get("/f1/schedule")
def get_schedule_by_year(year: int = Query(..., ge=2018, le=2030)):
    """Opsi dropdown Grand Prix dinamis sesuai tahun yang dipilih di frontend."""
    try:
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        events = [
            {
                "round": int(row["RoundNumber"]),
                "event_name": row["EventName"],
                "location": row["Location"],
                "country": row["Country"],
            }
            for _, row in schedule.iterrows()
        ]
        return {"year": year, "events": events}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Gagal memuat jadwal {year}: {e}")


@router.get("/f1/drivers")
def get_drivers_by_session(
    year: int = Query(..., ge=2018, le=2030),
    grand_prix: str = Query(...),
    session: str = Query("R"),
):
    """Opsi dropdown pembalap aktual pada GP & sesi tertentu."""
    try:
        f1_session = fastf1.get_session(year, grand_prix, session)
        f1_session.load(telemetry=False, laps=False, weather=False, messages=False)
        drivers = [
            {
                "code": row["Abbreviation"],
                "full_name": row["FullName"],
                "team": row["TeamName"],
            }
            for _, row in f1_session.results.iterrows()
            if row["Abbreviation"]
        ]
        return {"year": year, "grand_prix": grand_prix, "session": session, "drivers": drivers}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Gagal memuat daftar pembalap: {e}")


@router.post("/stream/control")
def apply_stream_filter(payload: StreamFilterRequest):
    """Mengirim filter pilihan user di frontend ke producer.py via Kafka."""
    try:
        producer = get_control_producer()
        data = payload.model_dump()
        if data["drivers"]:
            data["drivers"] = [d.strip().upper() for d in data["drivers"] if d.strip()]
        producer.send(KAFKA_CONTROL_TOPIC, value=data)
        producer.flush()
        return {"status": "applied", "filter": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal mengirim perintah ke Kafka: {e}")