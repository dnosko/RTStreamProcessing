from contextlib import asynccontextmanager
import sys
from datetime import datetime
from itertools import groupby
from operator import itemgetter
import redis
import sqlalchemy as db
from sqlalchemy import exc
from typing import List, Optional
from pymongo import MongoClient, ASCENDING
from fastapi import FastAPI, Query

import json
from schemas import schemas_collisions_polygons as _schemas
from utils_api.utils import map_user_to_device
from utils_api.database_utils import get_users_from_db
from utils_api.database_utils import get_users_devices, get_polygons


#conn_str_mongodb = "mongodb://user:pass@localhost:7017"
conn_str_mongodb = "mongodb://user:pass@mongodb:27017"
#client_mongodb = MongoClient(conn_str_mongodb)
#mongo_db = client_mongodb["db"]
#collisions_collection = mongo_db["collisions"]

redis_cache = redis.StrictRedis(host='redis', port=6379, db=1, decode_responses=True)
#redis_cache = redis.StrictRedis(host='localhost', port=6379, db=1, decode_responses=True)
engine = db.create_engine("postgresql://postgres:password@postgres:5432/data")
#engine = db.create_engine("postgresql://postgres:password@0.0.0.0:25432/data")

INTERNAL_SERVER_ERROR = 500


@asynccontextmanager
async def lifespan(api: FastAPI):

    client_mongodb = MongoClient(conn_str_mongodb)
    mongo_db = client_mongodb["db"]
    global collisions_collection
    collisions_collection = mongo_db["collisions"]
    # Load the cache
    try:
        records = get_users_from_db(engine)
        keys = [str(key_value[0]) for key_value in records]
        values = [str(key_value[1]) for key_value in records]
        redis_cache.mset(dict(zip(keys, values)))
    except (exc.DataError, exc.OperationalError) as e:
        print(e)
        sys.exit(1)
    yield
    # Clean cache
    redis_cache.flushdb()


api = FastAPI(lifespan=lifespan)


def str_to_point(point: str):
    if point is not None:
        newPoint = json.loads(point)
        return _schemas.Point(**newPoint)


## returns all polygons from database, can be filtered by category id
@api.get("/polygons/", response_model=List[_schemas.Polygon])
def polygons_on_map(valid: Optional[bool] = Query(None), category: Optional[int] = Query(None)):
    try:
        all_polygons = get_polygons(engine, valid=valid, category=category)
    except exc.DataError as e:
        descr = str(e.__doc__) + str(e.orig)
        return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    result = [_schemas.Polygon(id=row[0], creation=row[1], valid=row[2], category=row[3], fence=row[4]) for row in
              all_polygons]

    return result


# Aké jednotky sa nachádzali v oblasti definovanej polygonom v určitom časovom okne
@api.get("/collisions/history/", response_model=_schemas.CollisionsWithTimeQuery)
def history_collisions(time: str = Query(..., title="Time",
                                         description="The time parameter specifying the time range for querying collisions. Please split the from and to time with ;"),
                       polygons: Optional[List[int]] = Query(None, title="Polygons ids"),
                       user: Optional[List[int]] = Query(None, title="User ids")):
    try:
        # get ids of devices based on users
        users_devices = get_users_devices(user, engine, redis_cache)
    except exc.DataError as e:
        descr = str(e.__doc__) + str(e.orig)
        return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    mapping = map_user_to_device(users_devices)  # map the user id to device id

    times = time.split(';')
    start_time_str = times[0]
    end_time_str = times[1] if len(times) > 1 and times[1] else None

    # Convert start_time and optionally end_time to datetime
    start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%S")
    query_filter = {"collision_date_in": {"$gte": start_time}}

    if end_time_str:
        end_time = datetime.strptime(end_time_str, "%Y-%m-%dT%H:%M:%S")
        query_filter["collision_date_in"]["$lte"] = end_time

    if polygons:  # create select query with specified polygons
        query_filter["polygon"] = {"$in": polygons}

    if user:  # create select query with specified users
        keys = [int(device[1]) for device in users_devices]  # get device keys
        query_filter["device"] = {"$in": keys}

    filtered_documents = collisions_collection.find(query_filter)

    filtered_documents.sort('device', ASCENDING)

    # Group records by the specified attribute
    grouped_records = {key: list(group) for key, group in groupby(filtered_documents, key=itemgetter('device'))}

    r = [{'id_user': mapping.get(str(device)), 'id_device': device,
          'collisions': [{'id_polygon': rec['polygon'], 'inside': rec['inside'], 'enter_date': rec['collision_date_in'],
                          'exit_date': rec['collision_date_out'],
                          'enter_point': rec['collision_point_in']['coordinates'],
                          'exit_point': rec['collision_point_out']['coordinates'] if rec[
                                                                                         'collision_point_out'] is not None else None}
                         for rec in records]}
         for device, records in grouped_records.items()]

    return _schemas.CollisionsWithTimeQuery(time=time, collisions=r)


# Aké jednotky sa nachádzajú aktuálne v oblasti definovanej polygonom
@api.get("/collisions/current/", response_model=List[_schemas.InsideOfPolygon])
def currently_in_polygon(polygons: Optional[List[int]] = Query(None, title="Polygons ids"),
                         user: Optional[List[int]] = Query(None, title="User ids")):
    try:
        # get ids of devices based on users
        users_devices = get_users_devices(user, engine, redis_cache)
    except exc.DataError as e:
        descr = str(e.__doc__) + str(e.orig)
        return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    mapping = map_user_to_device(users_devices)  # map the user id to device id
    query_filter = {"inside": True}
    # query = f"SELECT device, polygon, collision_date_in FROM collisions_table where inside = true;"

    if polygons:  # create select query with specified polygons
        query_filter["polygon"] = {"$in": polygons}

    if user:  # create select query with specified users
        keys = [int(device[1]) for device in users_devices]  # get device keys
        query_filter["device"] = {"$in": keys}

    filtered_documents = collisions_collection.find(query_filter)
    filtered_documents.sort('device', ASCENDING)

    # Group records by the specified attribute
    grouped_records = {key: list(group) for key, group in groupby(filtered_documents, key=itemgetter('device'))}

    r = [{'id_user': mapping[str(device)], 'id_device': device,
          'collisions': [{'id_polygon': rec['polygon'], 'enter_date': rec['collision_date_in']} for rec in
                         records]}
         for device, records in grouped_records.items()]

    return r
