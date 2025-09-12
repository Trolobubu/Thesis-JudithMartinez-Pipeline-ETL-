from pydantic import BaseModel
from typing import Optional

class Route(BaseModel):
    id: str
    route_id: int
    agency_id: Optional[str] = None
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    route_desc: Optional[str] = None
    route_type: Optional[int] = None
    route_url: Optional[str] = None
    route_color: Optional[str] = None
    route_text_color: Optional[str] = None

class Trip(BaseModel):
    id: str
    route_id: int
    service_id: int
    trip_id: str
    trip_headsign: Optional[str] = None
    trip_short_name: Optional[int] = None
    direction_id: Optional[int] = None
    block_id: Optional[int] = None
    shape_id: Optional[str] = None
    trip_type: Optional[int] = None

class Stop(BaseModel):
    id: str
    stop_id: str
    stop_name: str
    stop_lat: float
    stop_lon: float
    stop_code: Optional[str] = None
    location_type: Optional[int] = None

class StopTime(BaseModel):
    id: str
    trip_id: str
    arrival_time: str
    departure_time: str
    stop_id: str
    stop_sequence: int
    stop_headsign: Optional[str] = None
    pickup_type: Optional[int] = None
    drop_off_type: Optional[int] = None
    shape_dist_traveled: Optional[float] = None

class Transfer(BaseModel):
    id: str
    from_stop_id: int
    to_stop_id: int
    transfer_type: Optional[int] = None
    min_transfer_time: Optional[int] = None
    from_route_id: Optional[int] = None
    to_route_id: Optional[int] = None
    from_trip_id: Optional[str] = None
    to_trip_id: Optional[str] = None
