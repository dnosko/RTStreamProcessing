from typing import Dict

import websockets
from datetime import datetime
import random
import json


# TODO pridat typy parametrov
# TODO config subor do ktoreho vlozit websocket uri...

class Generator:
    # Default values
    DEVICES = list(range(0, 10, 1))
    LIMIT_X = 100.0
    LIMIT_Y = 100.0
    URI = "ws://localhost:8001"

    STOP = False

    def __init__(self, devices=DEVICES, limitX=LIMIT_X, limitY=LIMIT_Y, uri=URI) -> None:
        self.devices = devices
        self.limitX = limitX
        self.limitY = limitY
        self.uri = uri
        self.websocket = None

    async def hello(self):
        """ Initiates handshake with the websocket server. """
        self.websocket = await websockets.connect(self.uri)

    @staticmethod
    def choose_point(end, start=1.0) -> float:
        """ Chooses random point in interval from start to end."""
        return random.uniform(start, end)

    def generate_mock_data(self) -> Dict:
        """ Generates random mock data from randomly chosen device, point and current timestamp. """
        data = {"DeviceID": random.choice(self.devices),
                "Point": (self.choose_point(self.limitX), self.choose_point(self.limitY)),
                "Timestamp": datetime.now().isoformat()}
        return data

    async def send_data(self):
        """ Sends periodically random mock data to websocket server."""
        if self.websocket:
            while not self.STOP:
                data = self.generate_mock_data()
                data_str = json.dumps(data)
                await self.websocket.send(data_str)
        else:
            print("Connection not established.")

    async def stop(self):
        """ Stops the generator and closes the websocket"""
        self.STOP = True
        if self.websocket:
            await self.websocket.close()
