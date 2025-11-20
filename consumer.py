from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.error import ConsumeError, KafkaError  # Added for error handling
import time
import random

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'orders'
DLQ_TOPIC = 'orders_dlq'
GROUP_ID = 'order_analytics_group'

# --- GLOBAL STATE FOR AGGREGATION ---
running_total = 0.0
total_count = 0

def get_dlq_producer(schema_str, schema_registry_client):
    """ Creates a producer specifically for the Dead Letter Queue """
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer
    }
    return SerializingProducer(producer_conf)

def process_order(order):
    """ 
    Simulates processing logic.
    CRITERIA: Real-time aggregation (running average).
    """
    # Simulate random processing failure (15% chance) to demonstrate Retry/DLQ
    if random.random() < 0.15: 
        raise Exception("Simulated Database Connection Error")

    global running_total, total_count
    
    price = order['price']
    running_total += price
    total_count += 1
    average = running_total / total_count
    
    print(f"SUCCESS: Processed Order {order['orderId']} (${price}) | Running Avg: ${average:.2f}")

def main():
    print("Starting Consumer...")
    
    # 1. Load Schema
    with open("order.avsc", "r") as f:
        schema_str = f.read()

    # 2. Configure Clients
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    }

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([TOPIC])

    # Initialize DLQ Producer
    dlq_producer = get_dlq_producer(schema_str, schema_registry_client)

    print(f"Listening to {TOPIC}...")

    try:
        while True:
            try:
                # Poll for messages
                msg = consumer.poll(1.0)
            except ConsumeError as e:
                # Handle "Unknown Topic" error gracefully
                if e.error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"Topic '{TOPIC}' not ready yet. Waiting for Producer...")
                    time.sleep(2)
                    continue
                elif e.error.code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    # Real error, raise it
                    print(f"Critical Consumer Error: {e}")
                    raise e
            
            if msg is None: continue
            
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            order = msg.value()
            key = msg.key()

            # --- CRITERIA: RETRY LOGIC ---
            max_retries = 3
            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                try:
                    process_order(order)
                    success = True
                except Exception as e:
                    retry_count += 1
                    print(f"WARN: Processing failed for Order {key} (Attempt {retry_count}/{max_retries}). Error: {e}")
                    time.sleep(1) # Backoff strategy

            # --- CRITERIA: DEAD LETTER QUEUE (DLQ) ---
            if not success:
                print(f"ERROR: Max retries exceeded for Order {key}. Sending to DLQ: {DLQ_TOPIC}")
                dlq_producer.produce(topic=DLQ_TOPIC, key=key, value=order)
                dlq_producer.flush()

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()