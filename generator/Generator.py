from typing import Dict

import websockets
from datetime import datetime, date, timezone
import random
import json



# TODO pridat typy parametrov
# TODO config subor do ktoreho vlozit websocket uri...

class Generator:
    # Default values
    DEVICES = list(range(0, 5000, 1))
    LIMIT_X = 100.0
    LIMIT_Y = 100.0
    URI = "ws://0.0.0.0:8001"

    STOP = False

    def __init__(self, devices=DEVICES, limitX=LIMIT_X, limitY=LIMIT_Y, uri=URI) -> None:
        self.devices = devices
        self.limitX = limitX
        self.limitY = limitY
        self.uri = uri
        self.websocket = None

    async def hello(self):
        """ Initiates handshake with the websocket server. """
        self.websocket = await websockets.connect(self.uri, ping_interval=5, ping_timeout=10)
        print("Connection started.")

    @staticmethod
    def choose_point(end, start=0.0) -> float:
        """ Chooses random point in interval from start to end."""
        return random.uniform(start, end)

    def generate_mock_data(self) -> Dict:
        """ Generates random mock data from randomly chosen device, point and current timestamp. """
        timestamp = int(datetime.now(timezone.utc).timestamp() * 1e6)

        data = {"id": random.choice(self.devices),
                "point": {"x": self.choose_point(self.limitX), "y": self.choose_point(self.limitY)},
                "timestamp": timestamp}
        return data

    async def send_data(self):
        """ Sends periodically random mock data to websocket server."""
        try:
            if self.websocket:
                while not self.STOP:
                    data = self.generate_mock_data()
                    data_str = json.dumps(data)
                    await self.websocket.send(data_str)
            else:
                print("Connection not established.")
        except websockets.exceptions.ConnectionClosedError:
            print("Connection stopped")
            #await self.stop()
            await self.hello()
            print("Connection restarted")
            await self.send_data()


    async def stop(self):
        """ Stops the generator and closes the websocket"""
        self.STOP = True
        if self.websocket:
            await self.websocket.close(code=1000, reason="Normal closure")

