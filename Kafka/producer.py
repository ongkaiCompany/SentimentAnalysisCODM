import findspark
findspark.init()

from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json

# Kafka broker configuration
hostname = '10.123.51.194'
bootstrap_servers = f'{hostname}:9092'

# Kafka topic
topic = 'positivec'

def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("KafkaProducer") \
        .config('spark.sql.warehouse.dir', 'hdfs:/user/hive/warehouse/') \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    # Switch to the sentiment_data database
    spark.sql('USE sentiment_data')

    # Assuming you have a DataFrame named dccleaned
    dccleaned = spark.sql("SELECT * FROM prediction_result")

    # Create a KafkaProducer instance
    producer = KafkaProducer(bootstrap_servers=[bootstrap_servers],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    try:
        # Produce messages to Kafka topic
        for row in dccleaned.collect():
            data_dict = row.asDict()
            comment = data_dict.get('Comment', '')  # Assuming 'comment' is a column in your DataFrame
            sentiment = data_dict.get('Sentiment', '')  # Assuming 'sentiment' is a column in your DataFrame
            if sentiment == 'Positive':
                producer.send(topic, value=data_dict)
                print(f'Positive comment: {comment}')

        print("Messages sent successfully to Kafka topic")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close producer
        producer.close()

        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    main()
