from kafka import KafkaConsumer
import json

TOPIC_NAME = "vedio_intrcisn"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="test-group",
    value_deserializer=lambda x:
    json.loads(x.decode("utf-8"))
)

for message in consumer:
    print('witing')
    print(message.value)
    