# Daša Nosková - xnosko05
# VUT FIT 2024

from pydantic import BaseModel


class Point(BaseModel):
    x: float
    y: float

class Location(BaseModel):
    user_id: int
    device_id: int
    point: Point
    timestamp: int

class User(BaseModel):
    user_id: int
    device_id: int