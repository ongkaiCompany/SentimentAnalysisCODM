import os
from kafka import KafkaConsumer
import json

# Kafka broker configuration
bootstrap_servers = '10.123.51.194:9092'

# Kafka topic
topic = 'positivec'

# Checkpoint folder location
checkpoint_folder = 'checkpoint'

# Create checkpoint folder if it doesn't exist
os.makedirs(checkpoint_folder, exist_ok=True)

# Create a KafkaConsumer instance
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

try:
    # Start consuming messages
    for message in consumer:
        # Assuming each message contains a JSON serialized dictionary
        data_dict = message.value
        print(f"Received message: {data_dict}")

        # Save checkpoint
        checkpoint_file = os.path.join(checkpoint_folder, f'checkpoint_{message.offset}.json')
        with open(checkpoint_file, 'w') as f:
            json.dump({'topic': topic, 'partition': message.partition, 'offset': message.offset}, f)
            print(f"Checkpoint saved: {checkpoint_file}")

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    # Close consumer
    consumer.close()
