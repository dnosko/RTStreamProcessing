from datetime import datetime
from typing import Optional, List

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


class PolygonEnter(BaseModel):
    id_polygon: int
    enter_date: datetime

class InsideOfPolygon(BaseModel):
    id_user: int
    id_device: int
    collisions: List[PolygonEnter]

class CollisionEvent(BaseModel):
    id_polygon: int
    inside: bool
    enter_date: datetime
    exit_date: Optional[datetime]
    enter_point: Point
    exit_point: Optional[Point]

class Collision(BaseModel):
    id_user: int
    id_device: int
    collisions: List[CollisionEvent]
    #id_polygon: int
    #inside: bool
    #enter_date: datetime
    #exit_date: Optional[datetime]
    #enter_point: Point
    #exit_point: Optional[Point]

class CollisionsWithTimeQuery(BaseModel):
    time: str
    collisions: List[Collision]
