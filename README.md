# Big Data Smart Factories

This repository contains the consumer pipeline code for an IoT Smart Manufacturing project. The solution uses Kafka for real-time data streaming and demonstrates how to:

- Consume messages using the `kafka.Consumer` client
- Process messages with basic feature engineering logic
- Run multiple producers in 2 company topics to consumers
- Train and Test Spark Models and process results from data consumed from kafka
- Add to Logger

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
Link to Datasets: `https://github.com/HumanCenteredTechnology/SME-Manufacturing-Dataset`


![image](https://github.com/user-attachments/assets/45949110-7817-4378-a103-304a7e751ea2)


---

### 3. Start the Consumers

There is a consumer notebook located in:
- `src/consumer/Kafka_integration.ipynb`

This launches instances for both companies that indicate different sensor values from different assets

![image](https://github.com/user-attachments/assets/678a1195-067c-42d3-ae66-a00f2d879b3f)

---

## Model Training and Preprocessing

![image](https://github.com/user-attachments/assets/a6ca88fa-8e1d-4173-a8ce-6bd8c4653500)

Preprocess time-series values to gain time features (`year`, `month`, `day`, `hour` and `ts_unix`). Based on each asset seperately.

- **Power Consumption:**  
  - Preprocess the input with Lag Features for Time series values;
  - Preprocess `power_avg` into log transform to handle spike outliers better;
  - Random Forest Regression;

- **Product Count Forecast:**  
  - Preprocess the input with Lag Features for Time series values;
  - Random Forest Regression in Company A and Unsupervised Gradient Boosting (**Future Work**);

 - **Next-State Forecast:**  
  - Preprocess the input with Lag Features for Time series values;
  - Random Forest Classification;

 - **Item Classification:**  
  - KMeans Clustering Grouping (K best silhouette score for each company)

There is the model notebook located in:
- `src/models/Train-Models.ipynb`

---

## License

This project is licensed under the MIT License.
