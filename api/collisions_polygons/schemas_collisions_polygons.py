from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Polygon(BaseModel):
    id: int
    creation: datetime
    valid: bool
    category: str
    fence: str

class Point(BaseModel):
    x: float
    y: float

class InsideOfPolygon(BaseModel):
    #user_id: int
    device_id: int
    polygon_id: int
    enter_date: datetime


class Collision(BaseModel):
    device_id: int
    polygon_id: int
    inside: bool
    enter_date: datetime
    exit_date: Optional[datetime]
    enter_point: Point
    exit_point: Optional[Point]
