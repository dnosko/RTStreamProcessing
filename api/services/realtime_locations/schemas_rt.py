from pydantic import BaseModel
from typing import Tuple

from sqlalchemy import DateTime


class Point(BaseModel):
    x: float
    y: float

class Location(BaseModel):
    user_id: int
    device_id: int
    point: Point
    timestamp: int
