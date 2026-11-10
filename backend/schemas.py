from pydantic import BaseModel
from datetime import datetime
from typing import List

class LocationSchema(BaseModel):
    x: float
    y: float

class MetadataSchema(BaseModel):
    session_id: str
    driver_code: str

class TelemetryResponse(BaseModel):
    timestamp: datetime
    metadata: MetadataSchema
    speed: float
    throttle: int
    brake: bool
    gear: int
    location: LocationSchema

class TelemetryListResponse(BaseModel):
    count: int
    data: List[TelemetryResponse]