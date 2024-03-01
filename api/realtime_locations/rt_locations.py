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
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
redis_cache =  redis.StrictRedis(host='localhost', port=6379, db=1, decode_responses=True)
engine = db.create_engine("postgresql://postgres:password@localhost:25432/data")

metadata = MetaData()
users = Table('users', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('device', Integer)
                   )


def parse(values, schema):
    """ Converts a list of values in format (user_id, {json result from redis})
    to given schema which has to expect device_id and user_id. """
    locations = []
    for user, value in values:
        data = json.loads(value)
        data['device_id'] = data.pop('id')
        location = schema(user_id=user, **data)
        locations.append(location)
    return locations

async def set_cache(data, key):
    await redis_cache.set(
        key,
        data
    )

async def get_cache(key):
    return await redis_cache.get(key)

# description="List of users for who you would like to get their actual location."
@api.get("/rt/locations/", response_model=List[_schemas.Location])
def get_actual_location(user: List[int] = Query(..., title="User IDs")):
    cached_device = get_cache(user) #TODO problem
    query = select([users]).where(users.c.id.in_(user))
    with engine.connect() as conn:
        try:
            # get ids of devices based on users
            result = conn.execute(query)
            records = result.fetchall()

            # get locations from redis and map user to the result
            values = [(user_id, redis_client.get(device_id)) for user_id, device_id in records]

            return parse(values, _schemas.Location)
        except exc.DataError as e:
            descr = str(e.__doc__) + str(e.orig)
            return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}


# returns all actual points
@api.get("/rt/points/", response_model=List[_schemas.Location])
def points_on_map():
    all_keys = redis_client.keys('*')

    query = select([users]).where(users.c.device.in_(all_keys))
    with engine.connect() as conn:
        result = conn.execute(query)
        records = result.fetchall()

    # map the user id to device id
    mapping = {f"{device_id}": user_id for user_id, device_id in records}

    # get records and map key (device id) to get user id
    all_records = [(mapping[key], redis_client.get(key)) for key in all_keys]

    return parse(all_records, _schemas.Location)