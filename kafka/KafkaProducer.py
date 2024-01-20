from confluent_kafka import Producer, KafkaException


class KafkaProducer:
    CONFIG = {}

    def __init__(self, topic, server, client_id):
        self.topic = topic
        self.server = server
        self.client_id = client_id
        self.CONFIG = {
            'bootstrap.servers': self.server,
            'client.id': self.client_id
        }
        self.producer = Producer(self.CONFIG)

    def produce_msg(self, msg):
        try:
            self.producer.produce(self.topic, value=msg)
        except KafkaException as e:
            print(f"Failed to produce message: {e}")

    def flush(self, timeout=5.0):
        self.producer.flush(timeout=timeout)

    def stop_producing(self):
        self.producer.flush()
        self.producer.close()


