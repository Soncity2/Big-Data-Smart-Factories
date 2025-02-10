# producer/simulate_producer.py
import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
from src.config.producer_config import KAFKA_PRODUCER_CONFIG

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f"Delivery failed for message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_company_a_data():
    """
    Generates random data for Company A.
    Schema: timestamp, machine_state, power_consumption, cycle_count
    """
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "machine_state": random.choice(["ON", "OFF", "IDLE"]),
        "power_consumption": round(random.uniform(10.0, 100.0), 2),
        "cycle_count": random.randint(0, 1000)
    }

def generate_company_b_data():
    """
    Generates random data for Company B.
    Schema: timestamp, operational_status, energy_consumption, production_rate
    """
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "operational_status": random.choice(["RUNNING", "STOPPED", "MAINTENANCE"]),
        "energy_consumption": round(random.uniform(5.0, 80.0), 2),
        "production_rate": random.randint(0, 500)
    }

if __name__ == '__main__':
    producer = Producer(KAFKA_PRODUCER_CONFIG)
    
    topics = {
        "company_A_topic": generate_company_a_data,
        "company_B_topic": generate_company_b_data
    }
    
    try:
        while True:
            for topic, data_generator in topics.items():
                data = data_generator()
                producer.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
            producer.flush()
            time.sleep(2)  # Produce messages every 2 seconds
    except KeyboardInterrupt:
        print("Producer interrupted.")
