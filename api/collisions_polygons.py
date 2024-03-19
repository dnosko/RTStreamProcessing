from contextlib import asynccontextmanager
import sys
import redis
import sqlalchemy as db
from sqlalchemy import select, exc
from typing import List, Optional

from fastapi import FastAPI, Query
import psycopg2 as pg
import json
from schemas import schemas_collisions_polygons as _schemas
from utils_api.utils import map_user_to_device, group_records_by_column
from utils_api.database_utils import add_to_select_in_list, get_users_from_db
from utils_api.database_utils import get_users_devices, get_polygons


conn_str = 'user=admin password=quest host=questdb port=8812 dbname=qdb'


redis_cache = redis.StrictRedis(host='redis', port=6379, db=1, decode_responses=True)
engine = db.create_engine("postgresql://postgres:password@postgres:5432/data")

INTERNAL_SERVER_ERROR = 500

@asynccontextmanager
async def lifespan(api: FastAPI):
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
# (for format see https://questdb.io/docs/reference/sql/where/#timestamp-and-date)
@api.get("/collisions/history/", response_model=_schemas.CollisionsWithTimeQuery)
def history_collisions(time: str = Query(..., title="Time", description="The time parameter specifying the time range for querying collisions. See https://questdb.io/docs/reference/sql/where/#timestamp-and-date "),
                       polygons: Optional[List[int]] = Query(None, title="Polygons ids"),
                       user: Optional[List[int]] = Query(None, title="User ids")):

    try:
        # get ids of devices based on users
        users_devices = get_users_devices(user, engine, redis_cache)
    except exc.DataError as e:
        descr = str(e.__doc__) + str(e.orig)
        return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    mapping = map_user_to_device(users_devices)  # map the user id to device id

    query = f"SELECT * FROM collisions_table where collision_date_in in \'{time}\';"

    if polygons:  # create select query with specified polygons
        query = add_to_select_in_list(query, polygons, "polygon")

    if user:  # create select query with specified users
        keys = [device[1] for device in users_devices]  # get device keys
        query = add_to_select_in_list(query, keys, "device")

    with pg.connect(conn_str) as connection:
        with connection.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()

            grouped = group_records_by_column(records, 0)

            r = [{'id_user': mapping[str(id_device)], 'id_device': id_device,
                  'collisions': [{'id_polygon': rec[1], 'inside': rec[2],  'enter_date': rec[3],
                                  'exit_date': rec[4], 'enter_point': str_to_point(rec[5]), 'exit_point': str_to_point(rec[6])} for rec in
                                 rest_of_record]}
                 for id_device, rest_of_record in grouped]
            """result = [_schemas.Collision(id_user=mapping[str(row[0])],
                                         id_device=row[0],
                                         id_polygon=row[1],
                                         inside=row[2],
                                         enter_date=row[3],
                                         exit_date=row[4],
                                         enter_point=str_to_point(row[5]),
                                         exit_point=str_to_point(row[6])
                                         ) for row in records]"""
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

    query = f"SELECT device, polygon, collision_date_in FROM collisions_table where inside = true;"

    if polygons:  # create select query with specified polygons
        query = add_to_select_in_list(query, polygons, "polygon")

    if user:  # create select query with specified users
        keys = [device[1] for device in users_devices]  # get device keys
        query = add_to_select_in_list(query, keys, "device")

    with pg.connect(conn_str) as connection:
        with connection.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()

            grouped = group_records_by_column(records, 0)
            r = [{'id_user': mapping[str(id_device)], 'id_device': id_device,
                  'collisions': [{'id_polygon': polygon, 'enter_date': date_in} for id_device, polygon, date_in in
                                 rest_of_record]}
                 for id_device, rest_of_record in grouped]

    return r
