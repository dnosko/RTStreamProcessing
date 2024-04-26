import asyncio
import websockets
from KafkaProducer import KafkaProducer


class ServerWS:
    FLUSH_INTERVAL = 10000

    def __init__(self, kafka_topic: str, bootstrap_servers: str, host: str, port: int) -> None:
        self.producer = KafkaProducer(kafka_topic, bootstrap_servers)
        self.host = host
        self.port = port
        self.flush_interval = self.FLUSH_INTERVAL
        self.msg_counter = 0

    async def accept_msg(self, websocket) -> None:
        """Callback function for accepting messages from clients."""
        try:
            while True:
                message = await websocket.recv()
                reply = f"Data received"
                await websocket.send(reply)

                self.msg_counter += 1
                self.producer.produce_msg(message)
                # flush interval for sending messages in resonable batches
                if self.msg_counter % self.flush_interval == 0:
                    self.producer.flush()
        except websockets.exceptions.ConnectionClosedOK:
            print("Connection closed")
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed probably because of disconnection.")
        finally:
            self.producer.flush()
            await websocket.close()

    async def run(self) -> None:
        """Runs websocket server on specified host and port."""
        print("Websocket started")
        async with websockets.serve(self.accept_msg, self.host, self.port):
            await asyncio.Future()
