# Start with a Python 3.9 image
FROM python:3.9-slim

# Set environment variables for Kafka server and topic (override these at runtime if needed)
ENV BOOTSTRAP_SERVERS localhost:29092
ENV KAFKA_TOPIC user-login

# Install necessary system dependencies for confluent_kafka
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libsasl2-dev \
    libz-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy your Python script into the container
COPY pipelines/fetch_producer_code.py /app/fetch_producer_code.py

# Set the working directory
WORKDIR /app

# Run the Python script
CMD ["python", "fetch_producer_code.py"]
