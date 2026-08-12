

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


with open(KAFKA_CA_CERT, "r") as f:
    ca_pem_content = f.read()

with open(KAFKA_SERVICE_CERT, "r") as f:
    service_cert_content = f.read()
    
with open(KAFKA_SERVICE_KEY, "r") as f:
    service_key_content = f.read()

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