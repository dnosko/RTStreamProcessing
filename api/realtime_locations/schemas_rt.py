from pydantic import BaseModel
from typing import Tuple

from sqlalchemy import DateTime

class Point (BaseModel):
    x: float
    y: float

class Location(BaseModel):
    id: int
    point: Point
    timestamp: int
