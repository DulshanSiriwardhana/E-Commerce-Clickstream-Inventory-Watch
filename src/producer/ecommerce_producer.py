import json
import time
import random
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

broker = os.environ.get('KAFKA_BROKER', 'localhost:9092')
topic  = os.environ.get('KAFKA_TOPIC',  'ecommerce-events')

products = [f"PROD_{i}" for i in range(1, 21)]
users    = [f"USER_{i}" for i in range(1, 101)]
events   = ['view', 'add_to_cart', 'purchase']


def generate():
    uid = random.choice(users)

    if random.random() < 0.1:
        pid = random.choice(["PROD_1", "PROD_2"])
        weights = [0.9, 0.09, 0.01] if pid == "PROD_1" else [0.5, 0.3, 0.2]
    else:
        pid = random.choice(products)
        weights = [0.6, 0.3, 0.1]

    etype = random.choices(events, weights=weights)[0]

    return {
        "event_id":   str(__import__('uuid').uuid4()),
        "user_id":    uid,
        "product_id": pid,
        "event_type": etype,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


def create_producer(retries: int = 10, delay: int = 5):
    """Create KafkaProducer with retry so we survive slow broker startup."""
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda m: json.dumps(m).encode('utf-8')
            )
            print(f"Connected to Kafka broker at {broker}", flush=True)
            return producer
        except NoBrokersAvailable:
            print(f"Broker not available (attempt {attempt}/{retries}), retrying in {delay}s …", flush=True)
            time.sleep(delay)

    raise RuntimeError(f"Could not connect to Kafka broker at {broker} after {retries} attempts.")


producer = create_producer()
print(f"Streaming events to topic '{topic}' …", flush=True)

while True:
    evt = generate()
    producer.send(topic, value=evt)
    print(evt, flush=True)
    time.sleep(random.uniform(0.1, 0.5))
