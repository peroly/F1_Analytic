from datetime import datetime, timezone
from typing import Any, Optional

def _to_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def _to_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(round(float(val)))
    except (ValueError, TypeError):
        return default

def clean_telemetry_data(raw_data: dict) -> Optional[dict]:
    if not isinstance(raw_data, dict):
        return None

    raw_time = raw_data.get("timestamp")
    if isinstance(raw_time, str):
        try:
            clean_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        except ValueError:
            clean_time = datetime.now(timezone.utc)
    elif isinstance(raw_time, datetime):
        clean_time = raw_time if raw_time.tzinfo else raw_time.replace(tzinfo=timezone.utc)
    else:
        clean_time = datetime.now(timezone.utc)

    race_info = raw_data.get("race_info") or {}
    year = _to_int(race_info.get("year"), 2024)
    grand_prix = str(race_info.get("grand_prix") or "Unknown").strip()
    session_type = str(race_info.get("session") or "R").strip().upper()

    gp_slug = grand_prix.lower().replace(" ", "_")
    session_id = f"{year}_{gp_slug}_{session_type.lower()}"
    driver_code = str(raw_data.get("driver") or "UNK").strip().upper()

    tel = raw_data.get("telemetry") or {}

    speed = _to_float(tel.get("speed"), 0.0)
    if speed < 0.0 or speed > 400.0:
        speed = 0.0

    throttle = max(0, min(100, _to_int(tel.get("throttle"), 0)))
    gear = max(0, min(8, _to_int(tel.get("gear"), 0)))

    raw_brake = tel.get("brake", False)
    clean_brake = bool(raw_brake in [1, "1", True, "True", "true"])

    return {
        "timestamp": clean_time,
        "metadata": {
            "session_id": session_id,
            "driver_code": driver_code,
            "year": year,
            "grand_prix": grand_prix,
            "session": session_type,
            "team": str(raw_data.get("team") or "Unknown").strip(),
        },
        "lap_number": _to_int(raw_data.get("lap_number"), 0),
        "compound": str(raw_data.get("compound") or "UNKNOWN").strip().upper(),
        "tyre_life": _to_int(raw_data.get("tyre_life"), 0),
        "session_time_sec": _to_float(raw_data.get("session_time_sec"), 0.0),
        "speed": round(speed, 2),
        "rpm": _to_int(tel.get("rpm"), 0),
        "gear": gear,
        "throttle": throttle,
        "brake": clean_brake,
        "drs": _to_int(tel.get("drs"), 0),
        "distance": round(_to_float(tel.get("distance"), 0.0), 2),
        "location": {
            "x": _to_float(tel.get("x"), 0.0),
            "y": _to_float(tel.get("y"), 0.0),
            "z": _to_float(tel.get("z"), 0.0),
        },
    }