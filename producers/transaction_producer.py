
import json
import os
import time
import random
import requests
import schedule
from confluent_kafka import Producer
from dotenv import load_dotenv
 
load_dotenv()
 
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "fantasyland-transactions")
API_BASE = "http://localhost:8001"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 5))
 
ENDPOINTS = [
    "/transactions/ticket-online",
    "/transactions/ticket-gate",
    "/transactions/food-sale",
    "/transactions/gift-store-sale",
]
 
 
def delivery_report(err, msg):
    if err:
        print(f"[Transactions Producer] Delivery FAILED: {err}")
    else:
        print(f"[Transactions Producer] → topic={msg.topic()} offset={msg.offset()}")
 
 
class TransactionsProducer:
    def __init__(self):
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
 
    def fetch_and_publish(self):
        endpoint = random.choice(ENDPOINTS)
        try:
            response = requests.get(f"{API_BASE}{endpoint}", timeout=3)
            response.raise_for_status()
            events = response.json()
 
            for event in events:
                self.producer.produce(
                    topic=KAFKA_TOPIC,
                    key=event.get("event_id"),
                    value=json.dumps(event),
                    callback=delivery_report,
                )
 
            self.producer.flush()
            print(f"[Transactions Producer] Published {len(events)} events from {endpoint}")
 
        except Exception as e:
            print(f"[Transactions Producer] ERROR: {e}")
 
    def run(self):
        print(f"[Transactions Producer] Starting — polling every {POLL_INTERVAL}s → topic: {KAFKA_TOPIC}")
        self.fetch_and_publish()
        schedule.every(POLL_INTERVAL).seconds.do(self.fetch_and_publish)
        while True:
            schedule.run_pending()
            time.sleep(1)
 
 
if __name__ == "__main__":
    TransactionsProducer().run()