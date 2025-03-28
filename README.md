# Big Data Smart Factories

This repository contains the consumer pipeline code for an IoT Smart Manufacturing project. The solution uses Kafka for real-time data streaming and demonstrates how to:

- Consume messages using the `confluent_kafka.Consumer` client
- Process messages with basic feature engineering logic
- Run four consumer instances (two for each company) in parallel

---

## Repository Structure

```
Big-Data-Smart_Factories/
├── README.md
├── requirements.txt
├── spark_model_utility.py
└── src/
    ├── models/
    │   └── Train-Models.ipynb
    ├── consumer/
    │   └── Kafka_integration.ipynb
    └── producer/
        ├── companyA_stream_data.ipynb
        └── companyB_stream_data.ipynb
```

---

## Requirements

- Python 3.x
- Kafka broker (default: `localhost:9092`)
- Python package: `confluent-kafka`

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Setup and Running

### 1. Start the Kafka Broker

If Kafka is installed, you can use an alias for easier access:

```bash
alias cdk='cd /usr/local/kafka/kafka_2.13-3.2.1'
cdk
```

Start ZooKeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties &
```

Start the Kafka server:

```bash
bin/kafka-server-start.sh config/server.properties &
```

Create topics for the two companies:

```bash
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Company_A
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Company_B
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

### 2. Start the Producers

There are two producer notebooks, one for each company, located in:

- `src/producer/companyA_stream_data.ipynb`
- `src/producer/companyB_stream_data.ipynb`

Each notebook simulates IoT asset data streaming for a respective company.

---

### 3. Start the Consumers

In a separate terminal, run:

```bash
python src/consumer/run_consumers.py
```

This launches four consumer instances:
- Consumer_A1
- Consumer_A2
- Consumer_B1
- Consumer_B2

Each subscribes to their respective Kafka topic and processes incoming data.

---

## Customization

- **Data Schemas:**  
  Update the `generate_company_a_data()` and `generate_company_b_data()` functions (typically in a `simulate_producer.py`) to align with your actual sensor schema.

- **Processing Logic:**  
  Modify the `process_message()` function in `consumer/consumer.py` to implement your custom transformation or feature engineering logic.

---

## License

This project is licensed under the MIT License.
