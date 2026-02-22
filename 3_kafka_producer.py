import pandas as pd
import json
import time
from confluent_kafka import Producer

# --- 1. CONFIGURATION ---
# Confluent Kafka uses a simple dictionary for configuration
conf = {
    'bootstrap.servers': '127.0.0.1:9092', # FORCED IPv4
    'client.id': 'stylestream-producer'
}

producer = Producer(conf)
TOPIC_NAME = 'user-clicks'

# --- 2. DELIVERY CALLBACK ---
# This function triggers automatically to confirm if a message was successfully delivered
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Emitted Event: User {json.loads(msg.value().decode('utf-8'))['user_id'][:8]}... clicked item")

# --- 3. LOAD DATA & STREAM ---
print("Loading local transaction sample...")
df_transactions = pd.read_csv("transactions_sample.csv", nrows=5000)

print(f"Starting simulated traffic stream to Kafka topic: '{TOPIC_NAME}'...")

for index, row in df_transactions.iterrows():
    # Construct the JSON payload
    event = {
        "user_id": str(row['customer_id']),
        "item_id": str(row['article_id']),
        "action": "click",
        "timestamp": time.time()
    }
    
    # Push to Kafka using the official library
    # We must explicitly convert the dictionary to a JSON string, then encode it to bytes
    producer.produce(
        TOPIC_NAME, 
        value=json.dumps(event).encode('utf-8'), 
        callback=delivery_report
    )
    
    # Trigger any available delivery report callbacks
    producer.poll(0)
    
    time.sleep(1)

# Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered.
producer.flush()