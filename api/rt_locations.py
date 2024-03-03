from contextlib import asynccontextmanager

import sqlalchemy as db
from sqlalchemy import select, exc
import redis
from fastapi import FastAPI, Query
from schemas import schemas_rt as _schemas
from typing import List
import json

from utils_api.tables import users
from utils_api.utils import map_user_to_device
from utils_api.database_utils import get_users_devices, get_users_from_db

INTERNAL_SERVER_ERROR = 500
NOT_FOUND_ERROR = 404
BAD_REQUEST_ERROR = 400

redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
redis_cache = redis.StrictRedis(host='redis', port=6379, db=1, decode_responses=True)
engine = db.create_engine("postgresql://postgres:password@postgres:5432/data")

@asynccontextmanager
async def lifespan(api: FastAPI):
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


api = FastAPI(lifespan=lifespan)

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



# description="List of users for who you would like to get their actual location."
@api.get("/rt/locations/", response_model=List[_schemas.Location])
def get_actual_location(user: List[int] = Query(None, title="User IDs")):

    try:
        users_devices = get_users_devices(user, engine, redis_cache)
        values = [(user_id, redis_client.get(device_id)) for user_id, device_id in users_devices]

        return parse(values, _schemas.Location)
    except exc.DataError as e:
        descr = str(e.__doc__) + str(e.orig)
        return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}


# returns list of all users and devices?
# TODO fix nech to vracia iba teda user, device a nie aj points a timestamp lebo to robi endpoint vyssie
@api.get("/rt/users/", response_model=List[_schemas.Location])
def points_on_map():
    all_keys = redis_client.keys('*')

    with engine.connect() as conn:
        query = select([users]).where(users.c.device.in_(all_keys))
        result = conn.execute(query)
        records = result.fetchall()

    mapping = map_user_to_device(records)  # map the user id to device id

    # get records and map key (device id) to get user id
    all_records = [(mapping[key], redis_client.get(key)) for key in all_keys]

    return parse(all_records, _schemas.Location)
