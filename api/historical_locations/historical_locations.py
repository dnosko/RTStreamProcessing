import sqlalchemy as db
from typing import List, Optional
from utils import create_trajectory_dict, map_trajectories_to_users,  map_user_to_device
from fastapi import FastAPI, Path, Query
import psycopg2 as pg

import schemas_historical as _schemas

from sqlalchemy import select, Table, Column, Integer, MetaData, exc, Float, DateTime, and_

conn_str = 'user=admin password=quest host=127.0.0.1 port=8812 dbname=qdb'

INTERNAL_SERVER_ERROR = 500
api = FastAPI()

# TODO rovnake ako v rt_locations, takze to prehodit do nejakeho zvlast suboru v nejakej zlozke ze db?
engine = db.create_engine("postgresql://postgres:password@localhost:25432/data")
engine_quest = db.create_engine("postgresql://admin:quest@localhost:8812/qdb")
metadata = MetaData()
users = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('device', Integer)
              )

locations = Table('locations_table', metadata,
                  Column('id', Integer),
                  Column('point_x', Float),
                  Column('point_y', Float),
                  Column('timestamp', DateTime)
                  )

## trajectory of user
## time can be specified like this time=%272024-02-07T17:26;1m (plus 1 minuta)
@api.get("/history/locations/{user}/", response_model=_schemas.SpecifiedTimeTrajectory)
def trajectory(user: int = Path(..., title="User ID"), time: str = Query('', title="Time window")):
    s = select(users.c.device).where(users.c.id == user)
    with engine.connect() as conn:
        try:
            # get ids of devices based on users
            result = conn.execute(s)
            device = result.fetchone()[0]
        except exc.DataError as e:
            descr = str(e.__doc__) + str(e.orig)
            return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    if time == '':
        query = f'SELECT id, point_x,point_y,timestamp FROM locations_table where id = {device};'
    else:
        query = f"SELECT id, point_x,point_y,timestamp FROM locations_table where id = {device} and timestamp in \'{time}\';"

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

    s = select([users])

    # if users are specified, query only the results that match
    if user:
        s = s.where(users.c.id.in_(user))
    # get users - device from database for mapping
    with engine.connect() as conn:
        try:
            # get ids of devices based on users
            result = conn.execute(s)
            records_user_device = result.fetchall()

        except exc.DataError as e:
            descr = str(e.__doc__) + str(e.orig)
            return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    if user:
        keys = [device[1] for device in records_user_device]  # get device keys
        devices_ids = ', '.join(str(id) for id in keys)
        query = f'SELECT * FROM locations_table where id in ({devices_ids}) and timestamp in \'{time}\' order by id;'
    else:
        query = f'SELECT * FROM locations_table where timestamp in \'{time}\' order by id;'
    with pg.connect(conn_str) as connection:

        with connection.cursor() as cur:
            cur.execute(query)
            history_locations_records = cur.fetchall()

    mapping = map_user_to_device(records_user_device)  # map the user id to device id

    trajectories = map_trajectories_to_users(history_locations_records, mapping)

    return _schemas.MultipleUsersTrajectory(in_time=time, trajectories=trajectories)
