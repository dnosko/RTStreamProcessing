import asyncio
import websockets


class Consumer:
    HOST = 'localhost'
    PORT = 8001
    def __init__(self):
        self.websocket_server = None

    async def start_websocket(self):
        self.websocket_server = await websockets.serve(self.accept_msg, self.HOST, self.PORT)
        print("Websocket started")

    async def accept_msg(self, websocket):
        try:
            while True:
                message = await websocket.recv()
                print(message)
        finally:
            await websocket.close()

    async def run(self):
        print("Websocket started")
        async with websockets.serve(self.accept_msg, self.HOST, self.PORT):
            await asyncio.Future()  # run forever


