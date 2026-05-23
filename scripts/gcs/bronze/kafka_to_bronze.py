import json
from kafka import KafkaConsumer
import pandas as pd
from io import BytesIO
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
from datetime import datetime
from config import (
    KAFKA_SERVICE_URL, KAFKA_SERVICE_CERT, 
    KAFKA_SERVICE_KEY, KAFKA_CA_CERT
)
#--------------------------------------

TOPIC_NAME = "video_interactions"
GROUP_ID = "video-interactions-group"

def convert_uuid_columns(df,uuid_columns):
    for col in uuid_columns:
        df[col]=df[col].astype(str)
    return df
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

        
       
        
        try:
                gcs_client = storage.Client(
                        project="end-to-end-data-streaming-platform",
                        credentials=AnonymousCredentials(),
                        client_options={
                            "api_endpoint": "http://localhost:4443"
                        }
                    )
                events = []
                for message in consumer:
                
                    events.append(message.value)
                    if len(events) >= 1000:
                        df_events = pd.DataFrame(events)
                        df_events = convert_uuid_columns(df_events,["user_id","video_id","event_id"])
                        buffer = BytesIO()
                        df_events.to_parquet(buffer,index=False)
                        buffer.seek(0)
                        today = datetime.now().strftime("%Y-%m-%d-%h")
                        
                        blob =gcs_client.bucket(f"end-to-end-streaming-data-platform").blob(
                        f"bronze/kafka/ingestion_data={today}/events.parquet"
                    )
                        blob.upload_from_file(buffer,content_type="application/octet-stream")
                        events.clear()
        except Exception as e:
                print(f"Batch Error: {e}")

                
    except Exception as e:
        print(f"Critical Error in Consumer: {e}")
    
    finally:
        
        if 'consumer' in locals():
            consumer.close()
            print("Kafka consumer connection closed.")

if __name__ == "__main__":
    run_consumer()






