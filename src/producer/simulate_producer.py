# producer/simulate_producer.py
import json
import time
import random
from confluent_kafka import Producer
from src.config.producer_config import KAFKA_PRODUCER_CONFIG

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f"Delivery failed for message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_machine_data(machine_id, topic):
    """Simulate machine-specific data."""
    try:
        if topic == "company_A_topic":
            return {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            "asset": f"machine_{machine_id}",
            "items": random.randint(0, 500),
            "status": random.choice([0, 1, 2, 3]),
            "power_avg": round(random.uniform(10.0, 100.0), 2),
            "cycle_time": random.randint(0, 600)
            }
        elif topic == "company_B_topic":
            return {
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                "asset": f"machine_{machine_id}",
                "status": random.choice(["Alarm", "Standby", "MachineOn", "Production", "Loading", "Tooling"]),
                "alarm_time": random.randint(0, 600),
                "loading_time": random.randint(0, 600),
                "tooling_time": random.randint(0, 600),
                "maintenance_time": random.randint(0, 600),
                "support_time": random.randint(0, 600),
                "power_avg": round(random.uniform(10.0, 100.0), 2),
                "power_max": round(random.uniform(10.0, 100.0), 2),
                "power_min": round(random.uniform(10.0, 100.0), 2)
            }
        else:
            raise ValueError(f"Invalid topic: {topic}. Supported topics: 'company_A_topic', 'company_B_topic'")
    except Exception as e:
        return {"error": str(e), "machine_id": machine_id, "topic": topic}

def produce_messages(machine_id,topic):
    """Send messages to Kafka topic."""
    while True:
        data = generate_machine_data(machine_id,topic)
        print(data)
        producer.produce(topic, json.dumps(data))
        producer.flush()
        time.sleep(1)

if __name__ == '__main__':
    # Kafka Producer Configuration
    producer = Producer(KAFKA_PRODUCER_CONFIG)

    topic = "company_B_topic"
    machine_id = 4
    
    try:
        produce_messages(machine_id,topic)
    except KeyboardInterrupt:
        print("Producer interrupted.")
