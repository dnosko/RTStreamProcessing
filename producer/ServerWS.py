import asyncio
import websockets
from producer.KafkaProducer import KafkaProducer

class ServerWS:
    # toto vsetko tahat z config suboru
    HOST = 'localhost'
    PORT = 8001
    CLIENTID = 'consumerXY' # toto asi idealne do configu? alebo nejako ze co ak ich bdue viac tych klienotv?
    TOPIC = 'new_locations'
    KAFKASERVER = 'localhost:9092'

    FLUSH_INTERVAL = 1000

    def __init__(self):
        self.producer = KafkaProducer(self.TOPIC, self.KAFKASERVER, self.CLIENTID)
        self.msg_counter = 0

    async def accept_msg(self, websocket):
        try:
            while True:
                message = await websocket.recv()
                self.msg_counter += 1
                self.producer.produce_msg(message)
                if self.msg_counter % self.FLUSH_INTERVAL == 0:
                    self.producer.flush()
        except websockets.exceptions.ConnectionClosedOK:
            print("Connection closed")
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed probably because of disconnection.")
        finally:
            self.producer.stop_producing()
            await websocket.close()

    async def run(self):
        print("Websocket started")
        async with websockets.serve(self.accept_msg, self.HOST, self.PORT):
            await asyncio.Future()


