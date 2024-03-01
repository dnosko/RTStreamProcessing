import sqlalchemy as db
from sqlalchemy import String, DateTime, Boolean, ForeignKey, select, Table, Column, Integer, MetaData
from typing import List, Optional

from fastapi import FastAPI, Query
import psycopg2 as pg
import json
import schemas_collisions_polygons as _schemas

conn_str = 'user=admin password=quest host=127.0.0.1 port=8812 dbname=qdb'

INTERNAL_SERVER_ERROR = 500
api = FastAPI()

# TODO rovnake ako v rt_locations, takze to prehodit do nejakeho zvlast suboru v nejakej zlozke ze db?
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


def strToPoint(point: str):
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
# TODO pridat mapping user-device
@api.get("/collisions/history/", response_model=List[_schemas.Collision])
def history_collisions(time: str, polygons: Optional[List[int]] = Query(None, title="Polygonds ids")):
    if polygons:
        string_polygons = ','.join(str(item) for item in polygons)
        query = f"SELECT * FROM collisions_table where polygon in ({string_polygons}) and collision_date_in in \'{time}\';"
    else:
        query = f"SELECT * FROM collisions_table where collision_date_in in \'{time}\';"

    with pg.connect(conn_str) as connection:
        with connection.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()

            result = [_schemas.Collision(device_id=row[0],
                                         polygon_id=row[1],
                                         inside=row[2],
                                         enter_date=row[3],
                                         exit_date=row[4],
                                         enter_point=strToPoint(row[5]),
                                         exit_point=strToPoint(row[6])
                                         ) for row in records]
    return result


# Aké jednotky sa nachádzajú aktuálne v oblasti definovanej polygonom
# TODO pridat mapping user - device
@api.get("/collisions/current/", response_model=List[_schemas.InsideOfPolygon])
def currently_in_polygon(polygons: Optional[List[int]] = Query(None, title="Polygons ids")):
    if polygons:
        string_polygons = ','.join(str(item) for item in polygons)
        query = f"SELECT device, polygon, collision_date_in FROM collisions_table where polygon in ({string_polygons}) and inside = true;"
    else:
        query = f"SELECT device, polygon, collision_date_in FROM collisions_table where inside = true;"

    with pg.connect(conn_str) as connection:
        with connection.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()

            result = [_schemas.InsideOfPolygon(device_id=row[0],
                                               polygon_id=row[1],
                                               enter_date=row[2]
                                               ) for row in records]
    return result


## Kto aktuálne prekročil hranice polygonu - nie som si ista ako to vyriesit
# TODO
@api.get("/collisions/new/", response_model=List[_schemas.Polygon])
def new_collisions(time: str, user: Optional[List[int]] = Query(None, title="User IDs")):
    pass
