# config/consumer_config.py
KAFKA_CONSUMER_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    # group.id will be set dynamically in each consumer instance
    'auto.offset.reset': 'earliest'
}
