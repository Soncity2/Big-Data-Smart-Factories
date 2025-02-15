from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType
from confluent_kafka import Consumer, KafkaError
from src.config.consumer_config import KAFKA_CONSUMER_CONFIG
import json


class KafkaConsumerWorker:
    def __init__(self, topic, group_id):
        self.topic = topic
        # Use a copy of the consumer config and set the group id
        config = KAFKA_CONSUMER_CONFIG.copy()
        config['group.id'] = group_id
        self.consumer = Consumer(config)
        self.consumer.subscribe([self.topic])

        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("KafkaSparkStreaming") \
            .getOrCreate()

    def poll_messages(self):
        """
        Poll messages from Kafka and return them as a list.
        """
        messages = []
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("End of partition reached.")
                    else:
                        print(f"Error: {msg.error()}")
                else:
                    messages.append(msg.value().decode('utf-8'))
        except KeyboardInterrupt:
            print("Consumer interrupted.")
        finally:
            self.consumer.close()
        return messages

    def process_with_spark(self, messages):
        """
        Convert Kafka messages into Spark DataFrame and apply transformations.
        """
        if not messages:
            return

        if self.topic == "company_A_topic":
            schema = StructType([
                ("ts", StringType()),
                ("asset", StringType()),
                ("items", IntegerType()),
                ("status", IntegerType()),
                ("power_avg", FloatType()),
                ("cycle_time", IntegerType())])
        elif self.topic == "company_B_topic":
            schema = StructType([
                ("ts" , StringType()),
                ("asset", StringType()),
                ("status", StringType()),
                ("alarm_time", IntegerType()),
                ("loading_time", IntegerType()),
                ("tooling_time", IntegerType()),
                ("maintenance_time", IntegerType()),
                ("support_time", IntegerType()),
                ("power_avg", FloatType()),
                ("power_max", FloatType()),
                ("power_min", FloatType())
            ])
        else:
            raise ValueError(f"Invalid topic: {self.topic}. Supported topics: 'company_A_topic', 'company_B_topic'")


        # Convert messages to DataFrame
        rdd = self.spark.sparkContext.parallelize(messages)
        df = self.spark.read.json(rdd, schema=schema)

        # Feature Engineering
        processed_df = df \
            .withColumn("production_rate", col("items") / col("cycle_time")) \
            .withColumn("downtime_ratio", (col("cycle_time") - col("items")) / col("cycle_time"))

        # Show processed data
        processed_df.show()

    def run(self):
        print(f"Starting Kafka Consumer for topic: {self.topic}")
        while True:
            messages = self.poll_messages()
            self.process_with_spark(messages)


# Example usage
if __name__ == "__main__":
    topic = "company_B_topic"
    consumer_worker = KafkaConsumerWorker(topic, group_id="consumer_group_1")
    consumer_worker.run()