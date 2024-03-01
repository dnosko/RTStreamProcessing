import sqlalchemy as db
from sqlalchemy import select, exc
from typing import List, Optional

from fastapi import FastAPI, Query
import psycopg2 as pg
import json
import schemas_collisions_polygons as _schemas
from utils import map_user_to_device, group_records_by_column, add_to_select_in_list
#from api.utils_api.utils import add_to_select_in_list, map_user_to_device, group_records_by_column
#from api.utils_api.tables import users, polygons, polygons_category
from sqlalchemy import String, DateTime, Boolean, ForeignKey, Table, Column, Integer, MetaData

conn_str = 'user=admin password=quest host=127.0.0.1 port=8812 dbname=qdb'

INTERNAL_SERVER_ERROR = 500
api = FastAPI()

engine = db.create_engine("postgresql://postgres:password@localhost:25432/data")

metadata = MetaData()
users = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('device', Integer)
              )

polygons_category = Table('category_polygons', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('name', String))

polygons = Table('polygons', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('creation', DateTime),
                 Column('valid', Boolean),
                 Column('category', Integer, ForeignKey('polygons_category.id')),
                 Column('fence', String))



def str_to_point(point: str):
    if point is not None:
        newPoint = json.loads(point)
        return _schemas.Point(**newPoint)


## returns all polygons from database, can be filtered by category id
@api.get("/polygons/", response_model=List[_schemas.Polygon])
def polygons_on_map(valid: Optional[bool] = Query(None), category: Optional[int] = Query(None)):
    query = select(
        [polygons.c.id, polygons.c.creation, polygons.c.valid, polygons_category.c.name.label('category_name'),
         polygons.c.fence]).select_from(
        polygons.join(polygons_category, polygons.c.category == polygons_category.c.id)
    )

    if category is not None:  # category specified
        query = query.where(polygons.c.category == category)
    if valid is not None:
        query = query.where(polygons.c.valid == valid)

    with engine.connect() as conn:
        result = conn.execute(query)
        all_polygons = result.fetchall()

    result = [_schemas.Polygon(id=row[0], creation=row[1], valid=row[2], category=row[3], fence=row[4]) for row in
              all_polygons]

    return result


# Aké jednotky sa nachádzali v oblasti definovanej polygonom v určitom časovom okne
@api.get("/collisions/history/", response_model=_schemas.CollisionsWithTimeQuery)
def history_collisions(time: str, polygons: Optional[List[int]] = Query(None, title="Polygons ids"),
                       user: Optional[List[int]] = Query(None, title="User ids")):
    # get ids of devices based on users
    with engine.connect() as conn:
        try:
            s = select([users])
            # if users are specified, query only the results that match
            if user:
                s = s.where(users.c.id.in_(user))
            result = conn.execute(s)
            records_user_device = result.fetchall()

        except exc.DataError as e:
            descr = str(e.__doc__) + str(e.orig)
            return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    mapping = map_user_to_device(records_user_device)  # map the user id to device id

    query = f"SELECT * FROM collisions_table where collision_date_in in \'{time}\';"

    if polygons:  # create select query with specified polygons
        query = add_to_select_in_list(query, polygons, "polygon")

    if user:  # create select query with specified users
        keys = [device[1] for device in records_user_device]  # get device keys
        query = add_to_select_in_list(query, keys, "device")

    with pg.connect(conn_str) as connection:
        with connection.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()
            # TODO urobit zgrupene podla user_id
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
    with engine.connect() as conn:
        try:
            s = select([users])
            # if users are specified, query only the results that match
            if user:
                s = s.where(users.c.id.in_(user))
            result = conn.execute(s)
            records_user_device = result.fetchall()

        except exc.DataError as e:
            descr = str(e.__doc__) + str(e.orig)
            return {"id": e.code, "description": descr, "http_response_code": INTERNAL_SERVER_ERROR}

    mapping = map_user_to_device(records_user_device)  # map the user id to device id

    query = f"SELECT device, polygon, collision_date_in FROM collisions_table where inside = true;"

    if polygons:  # create select query with specified polygons
        query = add_to_select_in_list(query, polygons, "polygon")

    if user:  # create select query with specified users
        keys = [device[1] for device in records_user_device]  # get device keys
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
