import sqlalchemy as db
from sqlalchemy import select, Table, Column, Integer, MetaData, exc
import redis
from fastapi import FastAPI, Path, Query
import schemas_rt as _schemas
from typing import Optional, List
import json

INTERNAL_SERVER_ERROR = 500
NOT_FOUND_ERROR = 404
BAD_REQUEST_ERROR = 400

api = FastAPI()
# TODO config
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
engine = db.create_engine("postgresql://postgres:password@localhost:25432/data")

metadata = MetaData()
users = Table('users', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('device', Integer)
                   )

def parse(values, schema):
    locations = []
    for value in values:
        data = json.loads(value)
        location = schema(**data)
        locations.append(location)
    return locations

# description="List of users for who you would like to get their actual location."
@api.get("/rt/locations/", response_model=List[_schemas.Location])
def get_actual_location(user: List[int] = Query(..., title="User IDs")):
    query = select(users.c.device).where(users.c.id.in_(user))
    with engine.connect() as conn:
        try:
            # get ids of devices based on users
            result = conn.execute(query)
            devices = result.fetchall()
            # flatten the result
            keys = [device[0] for device in devices]
            # get locations from redis
            values = redis_client.mget(keys)
            print(values)

            # jsonify
            return parse(values, _schemas.Location)
        except exc.DataError as e:
            descr = str(e.__doc__) + str(e.orig)
            return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}


@api.get("/rt/points/", response_model=List[_schemas.Location])
def points_on_map():

    all_keys = redis_client.keys('*')
    all_records = [(redis_client.get(key)) for key in all_keys]

    return parse(all_records, _schemas.Location)