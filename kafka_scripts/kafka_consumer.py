import json
from kafka import KafkaConsumer

from config import (
    KAFKA_SERVICE_URL, KAFKA_SERVICE_CERT, 
    KAFKA_SERVICE_KEY, KAFKA_CA_CERT
)

TOPIC_NAME = "video_interactions"
GROUP_ID = "video-interactions-group"

def run_consumer():
    try:
        
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_SERVICE_URL,
            security_protocol="SSL",
            ssl_cafile=KAFKA_CA_CERT,
            ssl_certfile=KAFKA_SERVICE_CERT,
            ssl_keyfile=KAFKA_SERVICE_KEY,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",  
            enable_auto_commit=True,       
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        
        print(f"Consumer started successfully. Listening to topic: '{TOPIC_NAME}'...")
        print("Waiting for events (Press Ctrl+C to stop)...")

        
        for message in consumer:
            event_data = message.value
            print(f"Received Event: {event_data}")
            
            

    except Exception as e:
        print(f"Critical Error in Consumer: {e}")
    
    finally:
        
        if 'consumer' in locals():
            consumer.close()
            print("Kafka consumer connection closed.")

if __name__ == "__main__":
    run_consumer()