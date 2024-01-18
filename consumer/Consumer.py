import asyncio
import websockets


class Consumer:
    HOST = 'localhost'
    PORT = 8001
    def __init__(self):
        self.websocket_server = None

    async def start_websocket(self):
        self.websocket_server = await websockets.serve(self.accept_msg, self.HOST, self.PORT)

    async def accept_msg(self, websocket):
        await websocket.accept()
        try:
            while True:
                message = await websocket.recv()
                print(message)
        finally:
            await websocket.close()

    async def run(self):
        # might be unneccesary?
        await asyncio.Future()

    async def stop(self):
        if self.websocket_server:
            self.websocket_server.close()
            await self.websocket_server.wait_closed()
            print("WebSocket server stopped.")

