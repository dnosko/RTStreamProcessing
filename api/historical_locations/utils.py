from datetime import datetime
from itertools import groupby
from operator import itemgetter


def create_trajectory_dict(timestamp:datetime, x:float, y:float) -> dict:
    return {'timestamp': timestamp, 'point': {'x': x, 'y': y}}

def map_user_to_device(records:list) -> dict:
    """ Maps user id to device id from database records, where first column is user_id and second is device_id.
    @param records: list of records in format [(user_id, device_id), ...]
    """
    # map the user id to device id
    return {f"{device_id}": user_id for user_id, device_id in records}

def group_trajectories(trajectories: list) -> list:
    """ Groups database records accordingly based on their device_id (first column in the records)
    @param trajectories: list of records (from database) [(device_id, x, y, timestamp),...]
    """
    device_id_column = 0  # first column in the records
    # Sort records by id
    trajectories.sort(key=itemgetter(device_id_column))

    # Group records by id
    grouped_records = groupby(trajectories, key=itemgetter(device_id_column))

    return grouped_records


def map_trajectories_to_users(records: list, mapping: dict) -> list:
    """ Maps records of trajectory (history locations) to predefined output and user accordingly to mapping parameter.
    @param records: list of records (from database) [(device_id, x, y, timestamp),...]
    @param mapping: dictionary with mapping of device_id:user_id
    """
    # group trajectories
    grouped_records = group_trajectories(records)
    # Convert grouped records to the desired format
    return [{'id_user': mapping[str(id_device)], 'id_device': id_device,
             'trajectory': [create_trajectory_dict(timestamp, x, y) for id_device, x, y, timestamp in rest_of_record]}
            for id_device, rest_of_record in grouped_records]
