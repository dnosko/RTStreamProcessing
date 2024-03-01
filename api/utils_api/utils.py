from datetime import datetime
from itertools import groupby
from operator import itemgetter


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


def create_trajectory_dict(timestamp: datetime, x: float, y: float) -> dict:
    return {'timestamp': timestamp, 'point': {'x': x, 'y': y}}


def map_user_to_device(records: list) -> dict:
    """ Maps user id to device id from database records, where first column is user_id and second is device_id.
    @:param records: list of records in format [(user_id, device_id), ...]
    """
    # map the user id to device id
    return {f"{device_id}": user_id for user_id, device_id in records}


def group_records_by_column(records: list, group_by_column:int=0) -> list:
    """ Groups database records accordingly based on group_by_column
    @:param records: list of records (from database) for example: [(device_id, x, y, timestamp),...]
    @:param group_by_column: position of column by which to group by the values in the provided records
    """
    # Sort records by id
    records.sort(key=itemgetter(group_by_column))

    # Group records by id
    grouped_records = groupby(records, key=itemgetter(group_by_column))

    return grouped_records


def map_trajectories_to_users(records: list, mapping: dict) -> list:
    """ Maps records of trajectory (history locations) to predefined output and user accordingly to mapping parameter.
    @:param records: list of records (from database) [(device_id, x, y, timestamp),...]
    @:param mapping: dictionary with mapping of device_id:user_id
    """
    # group trajectories
    grouped_records = group_records_by_column(records, 0)
    # Convert grouped records to the desired format
    return [{'id_user': mapping[str(id_device)], 'id_device': id_device,
             'trajectory': [create_trajectory_dict(timestamp, x, y) for id_device, x, y, timestamp in rest_of_record]}
            for id_device, rest_of_record in grouped_records]
