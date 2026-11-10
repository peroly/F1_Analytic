from fastapi import APIRouter, Query
from database import get_database
from schemas import TelemetryListResponse
from typing import Optional

router = APIRouter(prefix="/api/v1/telementry", tags=["Telementry"])
db = get_database()

@router.get("/latest", response_model=TelemetryListResponse)
def get_latest_telementry(
    limit: int = Query(default=20, ge=1, le=100),
    driver_code: Optional[str] = Query(default=None, description="Driver's code"),
    session_id: Optional[str] = Query(default=None, description="Race session ID")
):
    query = {}
    if driver_code:
        query["metadata.driver_code"] = driver_code.upper()
    if session_id:
        query["metadata.session_id"] = session_id

    cursor = db["telementry_live"].find(query, {"_id": 0}).sort("timestamp", -1).limit(limit)
    records = list(cursor)

    return {
        "counts": len(records),
        "data": records
    }