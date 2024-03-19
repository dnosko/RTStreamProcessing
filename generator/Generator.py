import time
import random
import json
import websockets
from typing import Dict, Tuple

from datetime import datetime, timezone


class Generator:
    start_ts = 0

    STOP = False

    def __init__(self, devices: int, limit_x: Tuple[float, float], limit_y: Tuple[float, float], uri: str,
                 limit_cnt: int = None) -> None:
        self.devices = list(range(0, devices, 1))
        self.limit_x = limit_x
        self.limit_y = limit_y
        self.uri = uri
        self.cnt = 0
        self.websocket = None
        self.limit_cnt = limit_cnt

    async def hello(self):
        """ Initiates handshake with the websocket server. """
        self.websocket = await websockets.connect(self.uri, ping_interval=5, ping_timeout=10)
        print("Connection started.")

    @staticmethod
    def choose_point(start: float, end: float) -> float:
        """ Chooses random point in interval from start to end."""
        return random.uniform(start, end)

    def generate_mock_data(self) -> Dict:
        """ Generates random mock data from randomly chosen device, point and current timestamp. """
        timestamp = int(datetime.now(timezone.utc).timestamp() * 1e6)

        data = {"id": random.choice(self.devices),
                "point":
                    {"x": self.choose_point(self.limit_x[0], self.limit_x[1]),
                     "y": self.choose_point(self.limit_y[0], self.limit_y[1])
                     },
                "timestamp": timestamp}
        return data

    def gen_data(self, limit: int):
        """Function for generating mock data for testing purposes. """
        while self.cnt < limit:
            self.cnt += 1
            data = self.generate_mock_data()

            if self.cnt == 1:
                self.start_ts = time.time()
                print(f"Starting generating at {self.start_ts}")

            yield json.dumps(data)

            if self.cnt == limit:
                end_ts = time.time()
                print(end_ts)
                print(f"Generated {limit} messages in {end_ts - self.start_ts} seconds")

    async def send_data(self):
        """ Sends periodically random mock data to websocket server."""
        if self.cnt == 0:
            self.start_ts = time.time()
            print(f"Starting generating at {self.start_ts}")
        try:
            if self.websocket:
                while not self.STOP:
                    data = self.generate_mock_data()
                    data_str = json.dumps(data)
                    await self.websocket.send(data_str)
                    await self.websocket.recv()
                    self.cnt += 1
                    # if max limit is set, stop generating after the limit
                    if self.limit_cnt is not None and self.cnt == self.limit_cnt:
                        await self.stop()
                        end_ts = time.time()
                        print(end_ts)
                        print(f"Generated {self.limit_cnt} messages in {end_ts - self.start_ts} seconds")
            else:
                print("Connection not established.")
        except websockets.exceptions.ConnectionClosedError:
            print("Connection stopped")
            # await self.stop()
            await self.hello()
            print("Connection restarted")
            await self.send_data()

    async def stop(self):
        """ Stops the generator and closes the websocket"""
        self.STOP = True
        if self.websocket:
            await self.websocket.close(code=1000, reason="Normal closure")
