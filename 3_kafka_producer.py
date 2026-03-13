from confluent_kafka import Producer
import pandas as pd
import time
import json
import random

# --- CONFIGURATION ---
TOPIC = "user-clicks"
conf = {'bootstrap.servers': '127.0.0.1:9092'}
producer = Producer(conf)

print("Loading data...")
# Load the transactions we generated earlier
df = pd.read_csv("transactions_sample.csv")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

print(f"Starting to produce events to topic '{TOPIC}'...")

try:
    # Loop through the CSV and send events
    for index, row in df.iterrows():
        # Create a random action to simulate real traffic weighting
        # 80% views, 15% carts, 5% purchases
        action = random.choices(
            ["view", "add_to_cart", "purchase"], 
            weights=[80, 15, 5]
        )[0]
        
        event = {
            "user_id": str(row['customer_id']),
            "item_id": str(row['article_id']),
            "action": action, 
            "timestamp": time.time()
        }
        
        producer.produce(
            TOPIC, 
            value=json.dumps(event).encode('utf-8'), 
            callback=delivery_report
        )
        producer.poll(0)
        
        # Print every 10 events just to show it's working
        if index % 10 == 0:
            print(f"Sent {action} event for item {event['item_id']}")
            
        time.sleep(0.5) # Send 2 events per second

except KeyboardInterrupt:
    print("Stopped by user.")
finally:
    producer.flush()