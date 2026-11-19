from typing import Optional
from fastapi import APIRouter, Query
from database import get_database

router = APIRouter(prefix="/api/v1/telemetry", tags=["Telemetry"])


@router.get("/latest")
def get_latest_telemetry(
    year: Optional[int] = Query(None, description="Filter tahun balapan, misal 2024"),
    grand_prix: Optional[str] = Query(None, description="Filter nama GP, misal Monza"),
    session: Optional[str] = Query(None, description="Filter sesi, misal R atau Q"),
    drivers: Optional[str] = Query(None, description="Pisahkan dengan koma, misal VER,LEC"),
    lap_number: Optional[int] = Query(None, description="Filter nomor lap tertentu"),
    limit: int = Query(100, ge=1, le=500),
):
    db = get_database()
    query_filter: dict = {}

    # Rakit session_id dinamis yang cocok dengan format output cleaner.py
    if year and grand_prix and session:
        gp_slug = grand_prix.strip().lower().replace(" ", "_")
        query_filter["metadata.session_id"] = f"{year}_{gp_slug}_{session.strip().lower()}"
    else:
        if year:
            query_filter["metadata.year"] = year
        if grand_prix:
            query_filter["metadata.grand_prix"] = grand_prix.strip()
        if session:
            query_filter["metadata.session"] = session.strip().upper()

    if drivers:
        driver_list = [d.strip().upper() for d in drivers.split(",") if d.strip()]
        if driver_list:
            query_filter["metadata.driver_code"] = {"$in": driver_list}

    if lap_number is not None:
        query_filter["lap_number"] = lap_number

    cursor = (
        db["telemetry_live"]
        .find(query_filter, {"_id": 0})
        .sort("timestamp", -1)
        .limit(limit)
    )
    records = list(cursor)
    records.reverse()

    return {"count": len(records), "filter": query_filter, "data": records}