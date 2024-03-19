import asyncio
import websockets
from KafkaProducer import KafkaProducer
from concurrent.futures import ThreadPoolExecutor
class ServerWS:
    # toto vsetko tahat z config suboru
    HOST = "0.0.0.0" #'localhost'
    PORT = 8001
    TOPIC = 'new_locations'
    KAFKASERVER = 'kafka1:19092' #'localhost:9092' #'kafka1:19092'

    FLUSH_INTERVAL = 10000

    def __init__(self):
        self.producer = KafkaProducer(self.TOPIC, self.KAFKASERVER)
        self.msg_counter = 0

    async def accept_msg(self, websocket):
        try:
            while True:
                message = await websocket.recv()
                reply = f"Data recieved"
                await websocket.send(reply)
                #message = self.generator.gen_data()
                self.msg_counter += 1
                print(self.msg_counter)
                self.producer.produce_msg(message)
                if self.msg_counter % self.FLUSH_INTERVAL == 0:
                    self.producer.flush()
                #await asyncio.get_event_loop().run_in_executor(self.executor, self.producer.flush)
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


