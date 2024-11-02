from confluent_kafka import Consumer
import os
import json
import pytest
import pandas as pd
import sys


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

def test_producer_processed_message_content():
    consumer.subscribe(['user-login-processed'])
    msg = consumer.poll(timeout=10)
    assert msg is not None, "No message received"
    
    message = json.loads(msg.value().decode('utf-8'))
    assert 'user_id' in message, "Processed message missing 'user_id'"
    assert 'timestamp' in message, "Processed message missing 'timestamp'"

def test_producer_aggregated_stats_content():
    consumer.subscribe(['locale-login-stats'])
    msg = consumer.poll(timeout=10)
    assert msg is not None, "No message received"
    
    message = json.loads(msg.value().decode('utf-8'))
    assert 'login_count' in message, "Aggregated stats missing 'login_count'"
    assert 'first_login' in message, "Aggregated stats missing 'first_login'"
    assert 'last_login' in message, "Aggregated stats missing 'last_login'"

# def test_clean_data():
#     data = {
#         'user_id': [1, 2, np.nan, 4, 4],
#         'locale': ['US', 'CA', 'US', 'CA', 'CA'],
#         'timestamp': [1633024800, 1633111200, 1633197600, 1633284000, 1633284000]  # epoch timestamps
#     }
#     df = pd.DataFrame(data)
    
#     expected_data = {
#         'user_id': [1.0, 2.0, 4.0],
#         'locale': ['US', 'CA', 'CA'],
#         'timestamp': pd.to_datetime([1633024800, 1633111200, 1633284000], unit='s')
#     }
#     expected_df = pd.DataFrame(expected_data)

#     cleaned_df = clean_data(df)

#     pd.testing.assert_frame_equal(cleaned_df.reset_index(drop=True), expected_df)
#     print("test_clean_data passed")


# def test_locale_aggregate_stats():
#     data = {
#         'user_id': [1, 2, 3, 4, 5],
#         'locale': ['US', 'CA', 'US', 'CA', 'US'],
#         'timestamp': pd.to_datetime([1633024800, 1633111200, 1633197600, 1633284000, 1633370400], unit='s')
#     }
#     df = pd.DataFrame(data)
    
#     expected_data = {
#         'locale': ['CA', 'US'],
#         'login_count': [2, 3],
#         'first_login': [pd.to_datetime(1633111200, unit='s'), pd.to_datetime(1633024800, unit='s')],
#         'last_login': [pd.to_datetime(1633284000, unit='s'), pd.to_datetime(1633370400, unit='s')]
#     }
#     expected_df = pd.DataFrame(expected_data).set_index('locale')

#     stats_df = locale_aggregate_stats(df)

#     pd.testing.assert_frame_equal(stats_df, expected_df)
#     print("test_locale_aggregate_stats passed")

