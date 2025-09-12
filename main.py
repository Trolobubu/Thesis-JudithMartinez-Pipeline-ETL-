from fastapi import FastAPI, HTTPException, Query
from bson import ObjectId
from typing import List
from database import db
from models import Stop, StopTime, Route, Trip, Transfer
import re

app = FastAPI(title="NMBS API", version="1.0.0")

def serialize_doc(doc):
    doc["id"] = str(doc["_id"])
    doc.pop("_id")
    return doc

@app.get("/stops", response_model=list[Stop])
def get_all_stops(limit: int = Query(100, description="Número máximo de paradas a devolver")):
    stops_cursor = db.stops.find().limit(limit)
    stops_list = [serialize_doc(stop) for stop in stops_cursor]
    return stops_list

@app.get("/trips/{route_id}", response_model=list[Trip])
def get_trips_by_route(route_id: int):
    trips_cursor = db.trips.find({"route_id": route_id})
    trips_list = [serialize_doc(trip) for trip in trips_cursor]

    if not trips_list:
        raise HTTPException(status_code=404, detail="No trips found for this route_id")

    return trips_list

@app.get("/routes", response_model=list[Route])
def get_all_routes(limit: int = Query(100, description="Número máximo de rutas a devolver")):
    routes_cursor = db.routes.find().limit(limit)
    routes_list = [serialize_doc(route) for route in routes_cursor]
    return routes_list

@app.get("/stops/{stop_id}", response_model=Stop)
def get_stop(stop_id: str):
    stop = db.stops.find_one({"stop_id": stop_id})
    if not stop:
        raise HTTPException(status_code=404, detail="Stop not found")
    return serialize_doc(stop)

@app.get("/trips/{trip_id}/stop_times", response_model=list[StopTime])
def get_stop_times_by_trip(trip_id: str):
    times = db.stop_times.find({"trip_id": trip_id}).sort("stop_sequence", 1)
    return [serialize_doc(t) for t in times]

@app.get("/trips/{trip_id}/stops", response_model=list[Stop])
def get_stops_for_trip(trip_id: str):
    times = list(db.stop_times.find({"trip_id": trip_id}))

    times.sort(key=lambda x: int(x["stop_sequence"]))

    stop_ids = [t["stop_id"] for t in times]

    stops = db.stops.find({"stop_id": {"$in": stop_ids}})
    stops_dict = {s["stop_id"]: serialize_doc(s) for s in stops}

    return [stops_dict[stop_id] for stop_id in stop_ids if stop_id in stops_dict]

@app.get("/stop_times/{stop_id}", response_model=list[StopTime])
def get_stop_times_by_stop(stop_id: str):
    stop_times_cursor = db.stop_times.find({"stop_id": stop_id})
    stop_times_list = [serialize_doc(st) for st in stop_times_cursor]

    if not stop_times_list:
        raise HTTPException(status_code=404, detail="No stop_times found for this stop_id")

    stop_times_list.sort(key=lambda x: x["stop_sequence"])
    return stop_times_list

@app.get("/trip/{trip_id}/stop_times", response_model=list[StopTime])
def get_stop_times_by_trip(trip_id: str):
    stop_times_cursor = db.stop_times.find({"trip_id": trip_id})
    stop_times_list = [serialize_doc(st) for st in stop_times_cursor]

    if not stop_times_list:
        raise HTTPException(status_code=404, detail="No stop_times found for this trip_id")

    stop_times_list.sort(key=lambda x: x["stop_sequence"])
    return stop_times_list

@app.get("/search/stops", response_model=List[Stop])
def search_stops(name: str = Query(..., description="Name or part of name")):
    regex = re.compile(name, re.IGNORECASE)
    stops_cursor = db.stops.find({"stop_name": {"$regex": regex}})
    stops_list = [serialize_doc(stop) for stop in stops_cursor]

    if not stops_list:
        raise HTTPException(status_code=404, detail="No stops found matching this name")

    return stops_list

@app.get("/search/routes", response_model=List[Route])
def search_routes(agency: str = Query(..., description="Agency id")):
    routes_cursor = db.routes.find({"agency_id": agency})
    routes_list = [serialize_doc(route) for route in routes_cursor]

    if not routes_list:
        raise HTTPException(status_code=404, detail="No routes found for this agency")

    return routes_list

@app.get("/transfers/{stop_id}", response_model=List[Transfer])
def get_transfers_from_stop(from_stop_id: str):
    transfers_cursor = db.transfers.find({"from_stop_id": int(from_stop_id)})
    transfers_list = [serialize_doc(t) for t in transfers_cursor]

    if not transfers_list:
        raise HTTPException(status_code=404, detail="No transfers found from this stop_id")

    return transfers_list