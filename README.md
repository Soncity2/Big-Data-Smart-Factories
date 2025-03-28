# Big Data Smart Factories

This repository contains the consumer pipeline code for an IoT Smart Manufacturing project. The solution uses Kafka for real-time data streaming and demonstrates how to:

- Consume messages using the \`confluent_kafka.Consumer\` client.
- Process messages with simple feature engineering logic.
- Run four consumer instances (two for each company) in parallel.

## Repository Structure

Big-Data-Smart_Factories/
├── README.md
├── requirements.txt
├── spark_model_utility.py
└── src/
      ├── models/
            └──  Train-Models.ipynb
      │
      ├── consumer/
      │     └── Kafka_integration.ipynb
      └── producer/
            ├── companyA_stream_data.ipynb
            └── companyB_stream_data.ipynb

## Requirements

- Python 3.x
- Kafka broker running (default configuration assumes \`localhost:9092\`)
- Python package: \`confluent-kafka\`

Install dependencies with:

\`\`\`bash
pip install -r requirements.txt
\`\`\`

## Setup and Running

### 1. Start the Kafka Broker

Ensure you have Kafka running on your machine or run the following code:

\`\`\`bash
alias cdk='cd /usr/local/kafka/kafka_2.13-3.2.1'
cdk
\`\`\`

Observe config/zookeeper.properties  file and Start ZooKeeper

\`\`\`bash
bin/zookeeper-server-start.sh    config/zookeeper.properties &
\`\`\`

Observe config/ server.properties  file and Start Kafka server

\`\`\`bash
bin/kafka-server-start.sh        config/server.properties &
\`\`\`

Create topics 'Company_A' and 'Company_B' and check if it is actually created

\`\`\`bash
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Company_A
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Company_B
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
\`\`\`

### 2. Start the Producers

2 notebooks that implicate 2 companies with different assets. Datasets:

![image](https://github.com/user-attachments/assets/9990f342-c4c3-463a-bc29-be121e2fdc46)

### 3. Start the Consumers

In another terminal, run:

\`\`\`bash
python consumer/run_consumers.py
\`\`\`

This will start four consumers (Consumer_A1, Consumer_A2, Consumer_B1, Consumer_B2) that subscribe to their respective topics and process incoming messages.

## Customization

- **Data Schemas:** Modify the functions \`generate_company_a_data()\` and \`generate_company_b_data()\` in \`simulate_producer.py\` to match the actual columns of your datasets.
- **Processing Logic:** Update the \`process_message()\` method in \`consumer/consumer.py\` to include your actual feature engineering or data transformation steps.

## License

This project is licensed under the MIT License.
