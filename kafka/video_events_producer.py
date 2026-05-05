import json
import uuid
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

#----------------------------------------
# Create the producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC_NAME = "video_interactions"
INTERACTIONS = ["like", "play", "complete", "pause"]
DEVICES = ["mobile", "tv", "web"]

#----------------------------------------
def generate_event(user_id, video_id) -> dict:
    interaction = random.choice(INTERACTIONS)
    event = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user_{user_id}",
        "video_id": f"video_{video_id}",
        "interaction_type": interaction,
        "watch_time_sec": random.randint(5, 120) if interaction in ["play", "complete"] else None,
        "device_type": random.choice(DEVICES)
    }
    return event

if __name__ == "__main__":
    for _ in range(1000):  # 
        event = generate_event(
            user_id=random.randint(1, 100), 
            video_id=random.randint(1, 50000)
        )
        producer.send(TOPIC_NAME, value=event)
    producer.flush()
    print("Events sent")
#---------------------------------------