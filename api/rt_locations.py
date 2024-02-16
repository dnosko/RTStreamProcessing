import sqlalchemy as db
from sqlalchemy import select, Table, Column, Integer, MetaData
import redis
from fastapi import FastAPI, Path, Query
import schemas_rt as _schemas
from typing import Optional, List
import json

api = FastAPI()
# TODO config
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
engine = db.create_engine("postgresql://postgres:password@localhost:25432/data")

metadata = MetaData()
users = Table('users', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('device', Integer)
                   )

def jsonify(text: tuple) -> list:
    """ Converts tuple to list of dictionaries. """
    a = [dict(r) for r in text]
    return a

# description="List of users for who you would like to get their actual location."
@api.get("/rt/locations/", response_model=List[_schemas.Location])
def get_actual_location(user: List[int] = Query(..., title="User IDs")):
    query = select(users.c.device).where(users.c.id.in_(user))
    with engine.connect() as conn:
        try:
            result = conn.execute(query)
            devices = result.fetchall()
            print(devices) # output [(0,), (4,)]
            keys = [device[0] for device in devices]
            values = redis_client.mget(keys)
            print(values)
            locations = []
            for value in values:
                data = json.loads(value)
                location = _schemas.Location(**data)
                locations.append(location)

            return locations
        except Exception as e:
            pass


@api.get("/rt/points/", response_model=_schemas.Location)
def points_in_map():
    pass
