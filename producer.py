import time
import random
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'orders'

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for User record {msg.key()}: {err}")
    else:
        print(f"User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}]")

# Load Avro Schema
with open("order.avsc", "r") as f:
    schema_str = f.read()

schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)

print("Producer started. Press Ctrl+C to stop.")

try:
    i = 1000
    while True:
        i += 1
        order = {
            "orderId": str(i),
            "product": random.choice(["Laptop", "Mouse", "Keyboard", "Monitor", "HDMI Cable"]),
            "price": round(random.uniform(10.0, 1500.0), 2)
        }
        
        print(f"Producing: {order}")
        producer.produce(topic=TOPIC, key=str(i), value=order, on_delivery=delivery_report)
        producer.flush()
        time.sleep(2) # Slow down to make it readable

except KeyboardInterrupt:
    print("Stopped by user")
except Exception as e:
    print(f"Error: {e}")