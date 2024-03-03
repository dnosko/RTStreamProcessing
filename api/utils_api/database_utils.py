import redis.exceptions
from sqlalchemy import exc, select
from .tables import users, polygons, polygons_category
import psycopg2 as pg


def get_polygons(engine, category: int = None, valid: bool = None):
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
        return result.fetchall()


# TODO urobit krajsie a dopisat komentar hihi
def get_users_devices(user: list, engine, redis_cache):
    """ Looks into cache for defined users and returns list of tuples containing user id and their assigned device id.
    ak je user none tak zavola automaticky vsetky hodnoty z db.. lebo empty list = vsetky hodnoty a nastavi aj cache rovno"""
    users_devices = []
    if user:
        devices = redis_cache.mget(user)

        users_devices = list(zip(user, devices))
        print(users_devices)
    # if some values were none (not in cache), take these keys and ask database and add to cache, if user is empty list,
    # then all values from database table users should be loaded and cached.
    none_keys = [key for key, value in users_devices if value is None]
    if none_keys or not user:
        missing_values = get_users_from_db(engine, none_keys)
        if missing_values:
            # set missing values to redis
            try:
                redis_cache.mset(dict(missing_values))
                print(missing_values)
            except redis.exceptions.DataError as e:
                print(e)
                return [(key, value) for key, value in users_devices if value is not None]

        users_devices = users_devices + missing_values

    # return only values that have valid ids. (aka not None)
    return [(key, value) for key, value in users_devices if value is not None]


def get_users_from_db(engine, user: list = None) -> list:
    """ Fetches table users from database. If user is specified, applies where in rule on the list of specified users.
    @:param engine: SQLAlchemy connection engine to database
    @:param user: list of users to filter
    """
    with engine.connect() as conn:
        s = select([users])
        # if users are specified, query only the results that match
        if user:
            s = s.where(users.c.id.in_(user))
        # get ids of devices based on users
        result = conn.execute(s)
        return result.fetchall()


def add_to_select_in_list(original_query: str, items: list, column: str) -> str:
    """ Takes select query and adds IN condition from list of items for specified database column.
    @:param original_query: select query (ends with ;)
    @:param  items: list of items for IN condition
    @:param column: column name
    """
    string_items = ','.join(str(item) for item in items)
    query = original_query[:-1]  # take the original query without the semicolon

    new_query = query + f" and {column} in ({string_items});"

    return new_query


def get_one_user_device_id(engine, user_id: int):
    """ Returns device id of a specified user (by user_id) from database table. """
    with engine.connect() as conn:
        s = select(users.c.device).where(users.c.id == user_id)
        result = conn.execute(s)
        return result.fetchone()[0]


def get_historical_locations_for_device(conn_str, device_id: int, time: str = None):
    """ Returns historical locations (trajectory) of specified device from database table.
        Time window can be specified with time parameter.
        @:param conn_str: database connection string
        @:param device_id: id of the device
        @:param time: time window. (for format see https://questdb.io/docs/reference/sql/where/#timestamp-and-date)
    """
    if time:
        query = f"SELECT id, point_x,point_y,timestamp FROM locations_table where id = {device_id} and timestamp in \'{time}\';"
    else:
        query = f'SELECT id, point_x,point_y,timestamp FROM locations_table where id = {device_id};'

    with pg.connect(conn_str) as connection:
        with connection.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
