import os
import json
import time
import uuid
import pandas as pd
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError


def clean_data(df):
    # Drop any rows with missing values
    df = df.dropna()

    # Drop any duplicate rows
    df = df.drop_duplicates()

    # Convert the timestamp to a datetime object
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

    return df

def locale_aggregate_stats(df):
    # Group the data by the 'locale' column
    stats = df.groupby('locale').agg({
        'user_id': 'count',
        'timestamp': ['min', 'max']
    })

    # Rename the columns
    stats.columns = ['login_count', 'first_login', 'last_login']

    return stats

def publish_to_topic(topic, messages):
    try:
        producer.produce(topic, key=str(uuid.uuid4()), value=json.dumps(messages))
        producer.flush()
        print(f"Published batch to {topic}: {messages}")
    except KafkaError as e:
        print(f"Failed to publish message: {e}")

bootstrap_servers = os.environ.get('BOOTSTRAP_SERVERS', 'localhost:29092')
topic = os.environ.get('KAFKA_TOPIC', 'user-login')
group_id = "user-login-consumer-group"

# Configure the Kafka consumer
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'  # Start from the beginning of the topic
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)
consumer.subscribe([topic])

print(f"Subscribed to topic: {topic}")

# define a producer to publish the data from to in another topic
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'linger.ms': 500,  # Wait up to 500 ms to batch messages
    'batch.size': 32768  # Adjust batch size as needed
}
producer = Producer(producer_config)
processed_topic = "user-login-processed"
stats_topic = "locale-login-stats"

publish_interval = 10
batch_size = 100

# Poll for new messages
try:
    # add timer to only consume every 10 seconds
    start_time = time.time()
    messages = []
    while True:
        msg = consumer.poll(timeout=1.0)  # Wait for a message

        if msg is None:
            # No message available right now
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"Reached end of partition at {msg.topic()}:{msg.partition()} - {msg.offset()}")
            else:
                print(f"Error: {msg.error()}")
            continue

        # Deserialize the JSON message
        message_value = msg.value().decode('utf-8')
        message = json.loads(message_value)

        # Process the message (in this case, print it)
        print(f"Consumed message: {message}")

        # message example: 
        # {'user_id': '08131166-08f9-4657-af94-d83316a33a1d', 'app_version': '2.3.0', 'ip': '207.146.162.253', 'locale': 'MI', 'device_id': '6d3b2d37-aa02-4009-9a75-2f3c7a1cd869', 'timestamp': 1730415831}
        # convert the timestamp to a datetime object
        messages.append(message)
        # publish every 10 seconds
        if time.time() - start_time > publish_interval or (len(messages) >= batch_size):
            # create a dataframe from the messages
            df = pd.DataFrame(messages)

            df = clean_data(df)
            stats = locale_aggregate_stats(df)

            # then publish to another topic
            publish_to_topic(processed_topic, df.to_json())
            publish_to_topic(stats_topic, stats.to_json())

            # reset the timer and messages
            start_time = time.time()
            messages = []

            print(f"Published message to another topic: {df.to_json()}")
            time.sleep(1)

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    # Close the consumer
    consumer.close()
