# consumer/consumer.py
import json
import random
from confluent_kafka import Consumer, KafkaError
from src.config.consumer_config import KAFKA_CONSUMER_CONFIG

class KafkaConsumerWorker:
    def __init__(self, topic, group_id, consumer_name):
        self.topic = topic
        self.consumer_name = consumer_name
        # Use a copy of the consumer config and set the group id
        config = KAFKA_CONSUMER_CONFIG.copy()
        config['group.id'] = group_id
        self.consumer = Consumer(config)
        self.consumer.subscribe([self.topic])
    
    def process_message(self, message):
        """
        Simulate feature engineering by parsing the JSON message and adding a new field.
        """
        try:
            data = json.loads(message.value().decode('utf-8'))
            # Example processing: add a field with a random aggregate value
            data['processed_value'] = round(sum(random.random() for _ in range(3)), 4)
            print(f"{self.consumer_name} processed message: {data}")
        except Exception as e:
            print(f"{self.consumer_name} error processing message: {e}")
    
    def run(self):
        print(f"{self.consumer_name} started, listening to topic: {self.topic}")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue  # no message received within timeout
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Reached end of partition
                        print(f"{self.consumer_name} reached end of partition.")
                    else:
                        print(f"{self.consumer_name} error: {msg.error()}")
                else:
                    self.process_message(msg)
        except KeyboardInterrupt:
            print(f"{self.consumer_name} interrupted.")
        finally:
            self.consumer.close()
