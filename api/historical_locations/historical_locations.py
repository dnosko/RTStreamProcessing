import sqlalchemy as db
from typing import List, Optional

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
@api.get("/history/locations/{user}/", response_model=_schemas.Trajectory)
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
        query = f'SELECT point_x,point_y,timestamp FROM locations_table where id = {device};'
    else:
        query = f"SELECT point_x,point_y,timestamp FROM locations_table where id = {device} and timestamp in \'{time}\';"


    with pg.connect(conn_str) as connection:

        with connection.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()

        trajectory = [{'timestamp': timestamp, 'point': {'x': x, 'y': y}} for
                          x, y, timestamp in
                          records]

        return _schemas.Trajectory(id=user, trajectory=trajectory)



## location of users in specified time
@api.get("/history/locations/", response_model=List[_schemas.Trajectory])
def trajectory(time: str, user: Optional[List[int]] = Query(None, title="User IDs")):

    if user:
        s = select(users.c.device).where(users.c.id.in_(user))
        with engine.connect() as conn:
            try:
                # get ids of devices based on users
                result = conn.execute(s)
                devices = result.fetchall()
                # flatten the result
                keys = [device[0] for device in devices]
                # get locations from redis
                devices_ids = ', '.join(str(id) for id in keys)
                query = f'SELECT * FROM locations_table where id in ({devices_ids}) and timestamp in {time} order by id;'
            except exc.DataError as e:
                descr = str(e.__doc__) + str(e.orig)
                return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    else:
        query = f'SELECT * FROM locations_table where timestamp in {time} order by id;'
    with pg.connect(conn_str) as connection:

        with connection.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()

    trajectories = [{'id': device, 'trajectory': [{'timestamp': timestamp, 'point': {'x': x, 'y': y}}]} for device, x, y, timestamp in
                    records]

    return trajectories
