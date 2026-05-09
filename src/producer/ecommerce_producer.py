import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime
import uuid
import os
from dotenv import load_dotenv

load_dotenv()

broker = os.environ.get('KAFKA_BROKER', 'localhost:9092')
topic = os.environ.get('KAFKA_TOPIC', 'ecommerce-events')

products = [f"PROD_{i}" for i in range(1, 21)]
users = [f"USER_{i}" for i in range(1, 101)]
events = ['view', 'add_to_cart', 'purchase']

def generate():
    uid = random.choice(users)
    
    if random.random() < 0.1:
        pid = random.choice(["PROD_1", "PROD_2"])
        if pid == "PROD_1":
            etype = random.choices(events, weights=[0.9, 0.09, 0.01])[0] 
        else:
            etype = random.choices(events, weights=[0.5, 0.3, 0.2])[0]
    else:
        pid = random.choice(products)
        etype = random.choices(events, weights=[0.6, 0.3, 0.1])[0]

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": uid,
        "product_id": pid,
        "event_type": etype,
        "timestamp": datetime.utcnow().isoformat()
    }

producer = KafkaProducer(
    bootstrap_servers=broker,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

print(f"starting stream to {topic}..")
while True:
    evt = generate()
    producer.send(topic, value=evt)
    print(evt)
    
    time.sleep(random.uniform(0.1, 0.5))
