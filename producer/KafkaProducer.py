import json

from confluent_kafka import Producer, KafkaException


class KafkaProducer:

    def __init__(self, topic, server, client_id):
        self.topic = topic
        self.server = server
        self.client_id = client_id
        self.config = {
            'bootstrap.servers': self.server,
            'client.id': self.client_id
        }
        self.producer = Producer(self.config)

    def produce_msg(self, msg):
        try:
            load_msg = json.loads(msg)
            self.producer.produce(self.topic, value=msg, key=str(load_msg['id']).encode('utf-8'))
        except KafkaException as e:
            print(f"Failed to produce message: {e}")

    def flush(self, timeout=10.0):
        self.producer.flush(timeout=timeout)

    def stop_producing(self):
        self.producer.flush()
        #self.producer.close()


