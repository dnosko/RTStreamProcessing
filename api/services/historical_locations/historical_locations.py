import sqlalchemy as db
from typing import List, Optional
from api.utils_api import create_trajectory_dict, map_trajectories_to_users, map_user_to_device, add_to_select_in_list
from fastapi import FastAPI, Path, Query
import psycopg2 as pg

import schemas_historical as _schemas

from sqlalchemy import select, exc

from api.utils_api.tables import users

conn_str = 'user=admin password=quest host=127.0.0.1 port=8812 dbname=qdb'

INTERNAL_SERVER_ERROR = 500
api = FastAPI()

engine = db.create_engine("postgresql://postgres:password@localhost:25432/data")
engine_quest = db.create_engine("postgresql://admin:quest@localhost:8812/qdb")


## trajectory of user
## time can be specified like this time=%272024-02-07T17:26;1m (plus 1 minuta)
@api.get("/history/locations/{user}/", response_model=_schemas.SpecifiedTimeTrajectory)
def trajectory(user: int = Path(..., title="User ID"), time: str = Query(None, title="Time window")):
    with engine.connect() as conn:
        try:
            s = select(users.c.device).where(users.c.id == user)
            # get ids of devices based on users
            result = conn.execute(s)
            device = result.fetchone()[0]
        except exc.DataError as e:
            descr = str(e.__doc__) + str(e.orig)
            return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}



    if time:
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
def trajectory(time: str, user: Optional[List[int]] = Query(None, title="User IDs")):
    # get users - device from database for mapping
    with engine.connect() as conn:
        try:
            s = select([users])
            # if users are specified, query only the results that match
            if user:
                s = s.where(users.c.id.in_(user))
            # get ids of devices based on users
            result = conn.execute(s)
            records_user_device = result.fetchall()

        except exc.DataError as e:
            descr = str(e.__doc__) + str(e.orig)
            return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    query = f'SELECT * FROM locations_table where timestamp in \'{time}\';'

    if user:
        keys = [device[1] for device in records_user_device]  # get device keys
        query = add_to_select_in_list(query, keys, "id")

    # add order by to query, skip semicolon from previously defined query
    query = query[:-1] + " order by id;"
    with pg.connect(conn_str) as connection:

        with connection.cursor() as cur:
            cur.execute(query)
            history_locations_records = cur.fetchall()

    mapping = map_user_to_device(records_user_device)  # map the user id to device id

    trajectories = map_trajectories_to_users(history_locations_records, mapping)

    return _schemas.MultipleUsersTrajectory(in_time=time, trajectories=trajectories)
