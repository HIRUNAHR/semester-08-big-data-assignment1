Kafka Order Processing System

Module: Big Data and Analytics (Semester 8)

Project Overview

This project implements a distributed system using Apache Kafka and Python to process simulated e-commerce orders. It demonstrates key stream processing concepts required by the assignment:

Avro Serialization: Uses Confluent Schema Registry to enforce data schemas.

Real-time Aggregation: Calculates a running average of order prices.

Fault Tolerance: Implements Retry Logic for temporary failures.

Dead Letter Queue (DLQ): Handles permanently failed messages by routing them to a separate topic.

System Architecture

Producer: Generates random order data, serializes it to Avro, and pushes to the orders topic.

Consumer: Subscribes to orders, deserializes data, performs aggregation, and handles errors.

Infrastructure: Docker Compose stack running Kafka, Zookeeper, and Schema Registry.

Prerequisites

Docker Desktop

Python 3.8+

Setup & Installation

Start Infrastructure

docker-compose up -d


Wait 30-60 seconds for the services to initialize.

Install Dependencies

pip install -r requirements.txt


How to Run

1. Start the Consumer (Analytics Engine)
Open a terminal and run:

python consumer.py


The consumer will wait for messages. Note: You may see intentional "Simulated Database Connection Error" logs. This is part of the assignment to demonstrate retry logic.

2. Start the Producer (Data Source)
Open a second terminal and run:

python producer.py


Assignment Proofs

Aggregation: Check the Running Avg log output in the Consumer terminal.

Retry Logic: Observe WARN logs indicating re-attempts on failure.

DLQ: Observe ERROR logs showing messages moving to orders_dlq after 3 failed attempts.

Project Structure

docker-compose.yml: Kafka ecosystem configuration.

order.avsc: Avro schema definition.

producer.py: Order generator script.

consumer.py: Main processing script with aggregation and error handling.

requirements.txt: Python dependencies.