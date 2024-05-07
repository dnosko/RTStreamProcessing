# Daša Nosková - xnosko05
# VUT FIT 2024

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
    id_user: int
    id_device: int
    trajectory: List[Location]

class SpecifiedTimeTrajectory(Trajectory):
    in_time: str

class MultipleUsersTrajectory(BaseModel):
    in_time: str
    trajectories: List[Trajectory]
