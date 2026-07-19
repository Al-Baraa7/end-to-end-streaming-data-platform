# import json
# from kafka import KafkaConsumer
# import pandas as pd
# from io import BytesIO
# from google.cloud import storage
# from google.auth.credentials import AnonymousCredentials
# from datetime import datetime
# from config import (
#     KAFKA_SERVICE_URL, KAFKA_SERVICE_CERT, 
#     KAFKA_SERVICE_KEY, KAFKA_CA_CERT
# )
# #--------------------------------------

# TOPIC_NAME = "video_interactions"
# GROUP_ID = "video-interactions-group"

# def convert_uuid_columns(df,uuid_columns):
#     for col in uuid_columns:
#         df[col]=df[col].astype(str)
#     return df
# def run_consumer():
#     try:
        
#         consumer = KafkaConsumer(
#             TOPIC_NAME,
#             bootstrap_servers=KAFKA_SERVICE_URL,
#             security_protocol="SSL",
#             ssl_cafile=KAFKA_CA_CERT,
#             ssl_certfile=KAFKA_SERVICE_CERT,
#             ssl_keyfile=KAFKA_SERVICE_KEY,
#             group_id=GROUP_ID,
#             auto_offset_reset="earliest",  
#             enable_auto_commit=True,       
#             value_deserializer=lambda x: json.loads(x.decode("utf-8"))
#         )
        
#         print(f"Consumer started successfully. Listening to topic: '{TOPIC_NAME}'...")
#         print("Waiting for events (Press Ctrl+C to stop)...")

        
       
        
#         try:
#                 gcs_client = storage.Client(
#                         project="end-to-end-data-streaming-platform",
#                         credentials=AnonymousCredentials(),
#                         client_options={
#                             "api_endpoint": "http://localhost:4443"
#                         }
#                     )
#                 events = []
#                 for message in consumer:
                
#                     events.append(message.value)
#                     if len(events) >= 1000:
#                         df_events = pd.DataFrame(events)
#                         df_events = convert_uuid_columns(df_events,["user_id","video_id","event_id"])
#                         buffer = BytesIO()
#                         df_events.to_parquet(buffer,index=False)
#                         buffer.seek(0)
#                         today = datetime.now().strftime("%Y-%m-%d-%h")
                        
#                         blob =gcs_client.bucket(f"end-to-end-streaming-data-platform").blob(
#                         f"bronze/kafka/ingestion_data={today}/events.parquet"
#                     )
#                         blob.upload_from_file(buffer,content_type="application/octet-stream")
#                         events.clear()
#         except Exception as e:
#                 print(f"Batch Error: {e}")

                
#     except Exception as e:
#         print(f"Critical Error in Consumer: {e}")
    
#     finally:
        
#         if 'consumer' in locals():
#             consumer.close()
#             print("Kafka consumer connection closed.")

# if __name__ == "__main__":
#     run_consumer()


# import json
# from kafka import KafkaConsumer
# import pandas as pd
# from io import BytesIO
# import boto3
# from botocore.client import Config
# from datetime import datetime
# from config import (
#     KAFKA_SERVICE_URL, KAFKA_SERVICE_CERT, 
#     KAFKA_SERVICE_KEY, KAFKA_CA_CERT
# )
# #--------------------------------------

# TOPIC_NAME = "video_interactions"
# GROUP_ID = "video-interactions-group"

# def convert_uuid_columns(df, uuid_columns):
#     for col in uuid_columns:
#         df[col] = df[col].astype(str)
#     return df

# def run_consumer():
#     try:
        
#         consumer = KafkaConsumer(
#             TOPIC_NAME,
#             bootstrap_servers=KAFKA_SERVICE_URL,
#             security_protocol="SSL",
#             ssl_cafile=KAFKA_CA_CERT,
#             ssl_certfile=KAFKA_SERVICE_CERT,
#             ssl_keyfile=KAFKA_SERVICE_KEY,
#             group_id=GROUP_ID,
#             auto_offset_reset="earliest",  
#             enable_auto_commit=True,       
#             value_deserializer=lambda x: json.loads(x.decode("utf-8"))
#         )
        
#         print(f"Consumer started successfully. Listening to topic: '{TOPIC_NAME}'...")
#         print("Waiting for events (Press Ctrl+C to stop)...")

#         try:
                
#                 minio_client = boto3.client(
#                     's3',
#                     endpoint_url='http://localhost:9000',
#                     aws_access_key_id='minio',            
#                     aws_secret_access_key='minio123',     
#                     config=Config(signature_version='s3v4')
#                 )
                
#                 events = []
#                 for message in consumer:
                
#                     events.append(message.value)
#                     if len(events) >= 1000:
#                         df_events = pd.DataFrame(events)
#                         df_events = convert_uuid_columns(df_events, ["user_id", "video_id", "event_id"])
#                         buffer = BytesIO()
#                         df_events.to_parquet(buffer, index=False)
#                         buffer.seek(0)
#                         today = datetime.now().strftime("%Y-%m-%d-%h")
                        
                        
#                         minio_client.upload_fileobj(
#                             buffer, 
#                             Bucket="end-to-end-streaming-data-platform-bronze", 
#                             Key=f"bronze/kafka/ingestion_data={today}/events.parquet"
#                         )
#                         events.clear()
#         except Exception as e:
#                 print(f"Batch Error: {e}")

                
#     except Exception as e:
#         print(f"Critical Error in Consumer: {e}")
    
#     finally:
        
#         if 'consumer' in locals():
#             consumer.close()
#             print("Kafka consumer connection closed.")

# if __name__ == "__main__":
#     run_consumer()

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from config import (
    KAFKA_SERVICE_URL, KAFKA_SERVICE_CERT, 
    KAFKA_SERVICE_KEY, KAFKA_CA_CERT
)

TOPIC_NAME = "video_interactions"

spark = SparkSession.builder \
    .appName("KafkaToMinIO_Bronze_Streaming") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
json_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("interaction_type", StringType(), True),
    StructField("watch_time_sec", StringType(), True),
    StructField("device_type", StringType(), True)
    
])

# 1. قراءة محتوى الشهادات النصي الصافي أولاً كما طلب Claude
with open(KAFKA_CA_CERT, "r") as f:
    ca_pem_content = f.read()

with open(KAFKA_SERVICE_CERT, "r") as f:
    service_cert_content = f.read()
    
with open(KAFKA_SERVICE_KEY, "r") as f:
    service_key_content = f.read()
# 2. تمرير المحتوى النصي الصريح للخيارات الصحيحة بدون كلمات location وبدون أخطاء أسماء طويلة
df_kafka_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVICE_URL) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.type", "PEM") \
    .option("kafka.ssl.truststore.certificates", ca_pem_content) \
    .option("kafka.ssl.endpoint.identification.algorithm", "") \
    .option("kafka.ssl.keystore.type", "PEM") \
    .option("kafka.ssl.keystore.certificate.chain", service_cert_content) \
    .option("kafka.ssl.keystore.key", service_key_content) \
    .load()
# df_kafka_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_SERVICE_URL) \
#     .option("subscribe", TOPIC_NAME) \
#     .option("startingOffsets", "earliest") \
#     .option("kafka.security.protocol", "SSL") \
#     .option("kafka.ssl.truststore.type", "PEM") \
#     .option("kafka.ssl.truststore.config.location", KAFKA_CA_CERT) \
#     .option("kafka.ssl.endpoint.identification.algorithm", "") \
#     .option("kafka.ssl.keystore.type", "PEM") \
#     .option("kafka.ssl.keystore.certificate.chain.config.location", KAFKA_SERVICE_CERT) \
#     .option("kafka.ssl.keystore.key.config.location", KAFKA_SERVICE_KEY) \
#     .load()

df_events = df_kafka_raw \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), json_schema).alias("data")) \
    .select("data.*")

df_events = df_events \
    .withColumn("user_id", col("user_id").cast("string")) \
    .withColumn("video_id", col("video_id").cast("string")) \
    .withColumn("event_id", col("event_id").cast("string"))

today = datetime.now().strftime("%Y-%m-%d-%h")
base_s3_path = f"s3a://end-to-end-streaming-data-platform-bronze/kafka/ingestion_data={today}"
checkpoint_path = f"s3a://end-to-end-streaming-data-platform-bronze/checkpoints/kafka_to_bronze"

query = df_events.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", base_s3_path) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()