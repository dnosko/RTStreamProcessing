import json

from confluent_kafka import Producer, KafkaException


class KafkaProducer:

    def __init__(self, topic: str, servers: str):
        self.topic = topic
        self.servers = servers
        self.config = {
            'bootstrap.servers': self.servers,
            'retries': 10,
            'acks': 'all',
            'enable.idempotence': True,
            'statistics.interval.ms': 250,
            'stats_cb': self.fetch_and_print_metrics
        }
        self.producer = Producer(self.config)

    def produce_msg(self, msg):
        try:
            load_msg = json.loads(msg)
            self.producer.produce(self.topic, value=msg, key=str(load_msg['id']).encode('utf-8'),
                                  callback=self.delivery_report)
        except KafkaException as e:
            print(f"Failed to produce message: {e}")

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")

    def flush(self):
        self.producer.flush()

    def fetch_and_print_metrics(self, stats_json_str):
        stats = json.loads(stats_json_str)
        keys_of_interest = ['msg_cnt', 'tx', 'rx', 'txretries', 'brokers','partitions']
        stats_chosen = {}

        for key in keys_of_interest:
            if key in stats:
                stats_chosen[key] = stats[key]


        print(json.dumps(stats_chosen, indent=4))
