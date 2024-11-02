# Kafka Data Pipeline with Producer and Consumer

This project sets up a data pipeline using Kafka, where a Python producer generates random user login events and sends them to a Kafka topic. A consumer then processes these messages, applies basic transformations or aggregations, and sends the processed data to another topic.

## Project Structure

- **Docker Compose**: Orchestrates services (Kafka, Zookeeper, Producer) in Docker containers.
- **Pipelines**: A Python-based Kafka producer that generates random login events, and a consumer that cleans and aggregates the data and produces to another topic.


## Requirements

- Docker & Docker Compose installed
- A Kafka broker and Zookeeper service (provided via Docker Compose)
- Python (for running the consumer locally if preferred)

## Quick Start

1. **Clone the Repository**: 
   ```bash
   git clone fetch_test
   cd fetch_test
    ```

2. **Start Docker Compose:**
    ```bash
    docker-compose up -d
    ```

3. **Test that the consumer is running with pytests:**
    ```bash 
    pytest tests/pipelines/test_user_login_pipeline.py
    ```

## Producer Details
The Python producer generates random login event messages with fields such as user_id, app_version, device_type, locale, ip, device_id, and timestamp. Occasionally, device_type is omitted to simulate real-world scenarios with missing fields.

## Producer Code Highlights
- Message Generation: Uses random values for fields such as IP address, locale, and device type.
- Topic Setup: Checks if the topic user-login exists and creates it if not.
- Message Publishing: Sends messages to the user-login topic at intervals of 0.5 seconds.

### Consumer Details
- The consumer reads messages from the user-login topic, processes the data (e.g., filtering, aggregating, or transforming fields), and publishes the processed data to a new topic. The consumer can handle cases with missing fields and will log any errors encountered.

## Error Handling
- Missing Fields: The consumer is designed to handle messages where fields are missing (e.g., device_type).
- Resilience: The Docker containers have restart policies, ensuring minimal downtime.
- Fault Tolerance: The consumer uses try-except blocks to handle exceptions and continue processing messages.
- Logging: The consumer logs errors and exceptions, making it easier to debug issues.

## Configuration Details
### Environment Variables
- The producer and consumer use environment variables for the Kafka configuration, making the setup easily adaptable:
- BOOTSTRAP_SERVERS: Specifies Kafka broker addresses.
- KAFKA_TOPIC: The primary topic for event messages.
- PROCESSED_TOPIC: Topic for processed messages (used by the consumer).

### Networking
Ports: Expose Kafka on 9092 and 29092 for internal and external access, respectively.

### Future Improvements
- **Monitoring**: Add monitoring tools like Prometheus and Grafana to track Kafka metrics.
- **Scaling**: Implement a multi-partition Kafka topic to handle higher message throughput.


_License
MIT License._

