# Kafka Order Processing System  
**Module:** Big Data and Analytics (Semester 8)

## Project Overview  
This project implements a distributed stream-processing system using Apache Kafka and Python to process simulated e-commerce orders. It demonstrates the key concepts required for the assignment:

- Avro Serialization using Confluent Schema Registry (via order.avsc)
- Real-time Aggregation of order prices (running average)
- Fault Tolerance through retry logic
- Dead Letter Queue (DLQ) to handle permanently failing messages (orders_dlq topic)

## System Architecture

### Producer (producer.py)
- Generates random order events
- Serializes data to Avro
- Publishes events to the orders Kafka topic

### Consumer (consumer.py)
- Subscribes to the orders topic
- Deserializes Avro messages
- Computes a running average of order prices
- Implements retry logic for temporary failures
- Sends unrecoverable messages to the DLQ (orders_dlq topic)

### Infrastructure
- Kafka Broker
- Zookeeper
- Confluent Schema Registry
- All services orchestrated using Docker Compose

## Prerequisites
- Docker Desktop
- Python 3.8+ version

## Setup & Installation

1. Clone the Repository
git clone https://github.com/HIRUNAHR/semester-08-big-data-assignment1
cd kafka-order-system

2. Start the Kafka Infrastructure
docker-compose up -d

3. Install Python Dependencies
pip install -r requirements.txt

## How to Run the System

You will need two separate terminal windows to run the producer and the consumer separately.

Terminal 1: Start the Consumer
python consumer.py

Terminal 2: Start the Producer
python producer.py

## Project Files

File                    Description
docker-compose.yml      Kafka, Zookeeper and Schema Registry configuration
order.avsc              Avro schema for Order objects
producer.py             Produces and serializes order events
consumer.py             Consumes, aggregates, and handles failures
requirements.txt        Python dependencies
