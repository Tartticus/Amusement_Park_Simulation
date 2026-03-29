
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
KAFKA_TOPIC = os.getenv("PARK_EVENTS_TOPIC", "fantasyland-arrivals")
API_BASE = os.getenv("FANTASYLAND_API_BASE", "http://localhost:8000")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 5))  # seconds

ENDPOINTS = [
    "/events/hotel-checkin",
    "/events/park-entry",
    "/events/queue-entry",
]


def delivery_report(err, msg):
    if err:
        print(f"[Producer] Delivery FAILED: {err}")
    else:
        print(f"[Producer] → topic={msg.topic()} offset={msg.offset()}")


class ParkArrivalProducer:
    def __init__(self):
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    def fetch_and_publish(self):
        endpoint = random.choice(ENDPOINTS)
        try:
            response = requests.get(f"{API_BASE}{endpoint}", timeout=3)
            response.raise_for_status()
            events = response.json()  # now a list of 1–50 events

            for event in events:
                self.producer.produce(
                    topic=KAFKA_TOPIC,
                    key=event.get("event_id"),
                    value=json.dumps(event),
                    callback=delivery_report,
                )

            self.producer.flush()
            print(f"[Producer] Published {len(events)} events from {endpoint}")

        except Exception as e:
            print(f"[Producer] ERROR: {e}")

    def run(self):
        print(f"[Producer] Starting — polling every {POLL_INTERVAL}s → topic: {KAFKA_TOPIC}")
        self.fetch_and_publish()
        schedule.every(POLL_INTERVAL).seconds.do(self.fetch_and_publish)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    ParkArrivalProducer().run()