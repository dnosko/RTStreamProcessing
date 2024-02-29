from datetime import datetime
from typing import List

from pydantic import BaseModel


class Point(BaseModel):
    x: float
    y: float


class Location(BaseModel):
    point: Point
    timestamp: datetime


class Trajectory(BaseModel):
    id: int
    trajectory: List[Location]
