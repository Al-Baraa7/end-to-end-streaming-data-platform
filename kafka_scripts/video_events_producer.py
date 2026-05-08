import json
import uuid
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
import psycopg2
from pymongo import MongoClient

from config import (
    DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, 
    DBM_HOST, DBM_PORT,
    KAFKA_SERVICE_URL, KAFKA_SERVICE_CERT, KAFKA_SERVICE_KEY, KAFKA_CA_CERT
)

TOPIC_NAME = "video_interactions"
INTERACTIONS = ["like", "play", "complete", "pause"]
DEVICES = ["mobile", "tv", "web"]

def get_user_ids():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users;")
        ids = [str(row[0]) for row in cur.fetchall()]
        cur.close()
        conn.close()
        if not ids:
            raise ValueError("Postgres: users table is empty")
        return ids
    except Exception as e:
        raise SystemExit(f"Postgres Connection Error: {e}")

def get_video_ids():
    try:
        client = MongoClient(host=DBM_HOST, port=DBM_PORT)
        db = client['platform_db']
        collection = db['videos']
        ids = [str(doc['video_id']) for doc in collection.find({}, {'video_id': 1})]
        client.close()
        if not ids:
            raise ValueError("MongoDB: videos collection is empty")
        return ids
    except Exception as e:
        raise SystemExit(f"MongoDB Connection Error: {e}")

def generate_event(user_id, video_id) -> dict:
    interaction = random.choice(INTERACTIONS)
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "video_id": video_id,
        "interaction_type": interaction,
        "watch_time_sec": random.randint(5, 120) if interaction in ["play", "complete"] else None,
        "device_type": random.choice(DEVICES)
    }

def run_producer():
    
    user_ids = get_user_ids()
    video_ids = get_video_ids()

    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVICE_URL,
            security_protocol="SSL",
            ssl_cafile=KAFKA_CA_CERT,
            ssl_certfile=KAFKA_SERVICE_CERT,
            ssl_keyfile=KAFKA_SERVICE_KEY,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    except Exception as e:
        raise SystemExit(f"Kafka Connection Error: {e}")

    
    try:
        print(f"Starting to send events to topic: {TOPIC_NAME}...")
        for _ in range(1000):
            event = generate_event(
                user_id=random.choice(user_ids),
                video_id=random.choice(video_ids)
            )
            producer.send(TOPIC_NAME, value=event)
        
        producer.flush()
        print("Success: 1000 events sent to Aiven Kafka.")
    except Exception as e:
        print(f"Runtime Error during production: {e}")
    finally:
        producer.close()
        print("Kafka producer connection closed.")

if __name__ == "__main__":
    run_producer()