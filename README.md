# Big Data Smart Factories

This repository contains the consumer pipeline code for an IoT Smart Manufacturing project. The solution uses Kafka for real-time data streaming and demonstrates how to:

- Consume messages using the \`confluent_kafka.Consumer\` client.
- Process messages with simple feature engineering logic.
- Run four consumer instances (two for each company) in parallel.

## Repository Structure

Big-Data-Smart_Factories/
├── README.md
├── requirements.txt
├── config/
│   ├── consumer_config.py
│   └── producer_config.py
├── consumer/
│   ├── __init__.py
│   ├── consumer.py
│   └── run_consumers.py
└── producer/
    ├── __init__.py
    └── simulate_producer.py

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

Ensure you have Kafka running on your machine or adjust the configuration files in \`config/\` accordingly.

### 2. Start the Producer (Data Simulation)

In one terminal, run:

\`\`\`bash
python producer/simulate_producer.py
\`\`\`

This script will simulate random messages for two companies based on their data schemas.

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
