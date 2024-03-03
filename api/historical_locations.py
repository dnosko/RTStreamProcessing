from contextlib import asynccontextmanager

import redis
import sqlalchemy as db
from typing import List, Optional
from utils_api.utils import create_trajectory_dict, map_trajectories_to_users, map_user_to_device
from utils_api.database_utils import add_to_select_in_list, get_users_devices, get_users_from_db, get_one_user_device_id
from fastapi import FastAPI, Path, Query
import psycopg2 as pg

from schemas import schemas_historical as _schemas

from sqlalchemy import  exc


conn_str = 'user=admin password=quest host=questdb port=8812 dbname=qdb'

engine = db.create_engine("postgresql://postgres:password@postgres:5432/data")
redis_cache = redis.StrictRedis(host='redis', port=6379, db=1, decode_responses=True)


@asynccontextmanager
async def lifespan(api: FastAPI):
    # Load the cache
    try:
        records = get_users_from_db(engine)
        keys = [str(key_value[0]) for key_value in records]
        values = [str(key_value[1]) for key_value in records]
        redis_cache.mset(dict(zip(keys, values)))
    except exc.DataError as e:
        pass
    yield
    # Clean cache
    redis_cache.flushdb()


INTERNAL_SERVER_ERROR = 500
api = FastAPI(lifespan=lifespan)


## trajectory of user
## time can be specified like this time=%272024-02-07T17:26;1m (plus 1 minuta)
@api.get("/history/locations/{user}/", response_model=_schemas.SpecifiedTimeTrajectory)
def trajectory(user: int = Path(..., title="User ID"),
               time: str = Query("all", title="Time window", description="The time parameter specifying the time range(window). See https://questdb.io/docs/reference/sql/where/#timestamp-and-date ")):
    try:
        device = get_one_user_device_id(engine, user)
    except exc.DataError as e:
        descr = str(e.__doc__) + str(e.orig)
        return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    if time != "all":
        query = f"SELECT id, point_x,point_y,timestamp FROM locations_table where id = {device} and timestamp in \'{time}\';"
    else:
        query = f'SELECT id, point_x,point_y,timestamp FROM locations_table where id = {device};'

    with pg.connect(conn_str) as connection:

        with connection.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()

        trajectory = [create_trajectory_dict(timestamp, x, y) for
                      id_device, x, y, timestamp in
                      records]

        return _schemas.SpecifiedTimeTrajectory(in_time=time, id_user=user, id_device=device, trajectory=trajectory)


## location of users in specified time
@api.get("/history/locations/", response_model=_schemas.MultipleUsersTrajectory)
def trajectory(time: str = Query(..., title="Time window", description="The time parameter specifying the time range(window). See https://questdb.io/docs/reference/sql/where/#timestamp-and-date "),
               user: Optional[List[int]] = Query(None, title="User IDs")):
    try:
        # get ids of devices based on users
        users_devices = get_users_devices(user, engine, redis_cache)
    except exc.DataError as e:
        descr = str(e.__doc__) + str(e.orig)
        return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    query = f'SELECT * FROM locations_table where timestamp in \'{time}\';'

    if user:
        keys = [device[1] for device in users_devices]  # get device keys
        query = add_to_select_in_list(query, keys, "id")

    # add order by to query, skip semicolon from previously defined query
    query = query[:-1] + " order by id;"
    with pg.connect(conn_str) as connection:

        with connection.cursor() as cur:
            cur.execute(query)
            history_locations_records = cur.fetchall()

    mapping = map_user_to_device(users_devices)  # map the user id to device id

    trajectories = map_trajectories_to_users(history_locations_records, mapping)

    return _schemas.MultipleUsersTrajectory(in_time=time, trajectories=trajectories)
