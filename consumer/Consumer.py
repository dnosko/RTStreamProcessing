import asyncio
import websockets


class Consumer:
    HOST = 'localhost'
    PORT = 8001

    def __init__(self):
        pass

    async def accept_msg(self, websocket):
        try:
            while True:
                message = await websocket.recv()
                print(message)
        except websockets.exceptions.ConnectionClosedOK:
            print("Connection closed")
        finally:
            await websocket.close()

    async def run(self):
        print("Websocket started")
        async with websockets.serve(self.accept_msg, self.HOST, self.PORT):
            await asyncio.Future()


