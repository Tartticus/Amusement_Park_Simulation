import json
import os
import boto3
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()



KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_BUCKET               = os.getenv("S3_BUCKET_FANTASY")               
AWS_REGION              = os.getenv("AWS_REGION", "us-east-2")
AWS_ACCESS_KEY_ID       = os.getenv("AWS_ACCESS_KEY_ID_FANTASY")
AWS_SECRET_ACCESS_KEY   = os.getenv("AWS_SECRET_ACCESS_KEY_FANTASY")

TOPICS = [
    "fantasyland-arrivals",
    "fantasyland-transactions",
]

# Map topic name to S3 folder prefix
TOPIC_PREFIX = {
    "fantasyland-arrivals":     "arrivals",
    "fantasyland-transactions": "transactions",
}




s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

print(f"[S3 Consumer] Connected to S3 bucket: {S3_BUCKET}")




def build_s3_key(topic: str, event: dict) -> str:
    """
    Build the S3 object key using Hive-style time partitioning.

    Example:
      arrivals/year=2026/month=03/day=29/hour=14/PARK_ENTRY_abc123.json
    """
    # Use event timestamp if available, otherwise now
    raw_ts = event.get("timestamp") or event.get("timestamp_polled")
    try:
        ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    except Exception:
        ts = datetime.now(timezone.utc)

    prefix     = TOPIC_PREFIX.get(topic, topic)
    event_type = event.get("event_type", "UNKNOWN")
    event_id   = event.get("event_id", "no_id")

    partition_path = (
        f"year={ts.year}/"
        f"month={ts.month:02d}/"
        f"day={ts.day:02d}/"
        f"hour={ts.hour:02d}"
    )

    filename = f"{event_type}_{event_id}.json"

    return f"{prefix}/{partition_path}/{filename}"


def write_to_s3(topic: str, event: dict):
    """Write a single event as a JSON file to S3."""
    key  = build_s3_key(topic, event)
    body = json.dumps(event, indent=2)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[S3 Consumer] Written → s3://{S3_BUCKET}/{key}")



class S3Consumer:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
            "group.id":           "fantasyland-s3-consumer",
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": False,   # manual commit — only after S3 write succeeds
        })
        self.consumer.subscribe(TOPICS)
        print(f"[S3 Consumer] Subscribed to: {TOPICS}")

    def run(self):
        print(f"[S3 Consumer] Listening for events...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"[S3 Consumer] Kafka error: {msg.error()}")
                    continue

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    topic = msg.topic()

                    write_to_s3(topic, event)

                    # Only commit offset AFTER successful S3 write
                    # If S3 write fails the message will be reprocessed
                    self.consumer.commit(message=msg)

                except Exception as e:
                    print(f"[S3 Consumer] ERROR processing message: {e}")

        except KeyboardInterrupt:
            print("[S3 Consumer] Shutting down...")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    S3Consumer().run()