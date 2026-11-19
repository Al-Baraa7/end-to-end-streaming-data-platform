
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import (
#     IntegerType,
#     StringType,
#     StructField,
#     StructType,
# )

# from config import (
#     KAFKA_SERVICE_URL,
#     KAFKA_SERVICE_CERT,
#     KAFKA_SERVICE_KEY,
#     KAFKA_CA_CERT,
# )


# TOPIC_NAME = "video_interactions"

# INGESTION_TIME = datetime.now().strftime("%Y-%m-%d-%h")

# BRONZE_PATH = (
#     "s3a://end-to-end-streaming-data-platform-bronze/"
#     f"kafka/ingestion_data={INGESTION_TIME}"
# )

# CHECKPOINT_PATH = (
#     "s3a://end-to-end-streaming-data-platform-bronze/"
#     "checkpoints/kafka_to_bronze"
# )


# spark = (
#     SparkSession.builder
#     .appName("KafkaToMinIO_Bronze_Streaming")
#     .config(
#         "spark.hadoop.fs.s3a.endpoint",
#         "http://minio:9000",
#     )
#     .config(
#         "spark.hadoop.fs.s3a.access.key",
#         "minio",
#     )
#     .config(
#         "spark.hadoop.fs.s3a.secret.key",
#         "minio123",
#     )
#     .config(
#         "spark.hadoop.fs.s3a.path.style.access",
#         "true",
#     )
#     .config(
#         "spark.hadoop.fs.s3a.impl",
#         "org.apache.hadoop.fs.s3a.S3AFileSystem",
#     )
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("ERROR")


# json_schema = StructType([
#     StructField(
#         "event_id",
#         StringType(),
#         True,
#     ),
#     StructField(
#         "event_timestamp",
#         StringType(),
#         True,
#     ),
#     StructField(
#         "user_id",
#         StringType(),
#         True,
#     ),
#     StructField(
#         "video_id",
#         StringType(),
#         True,
#     ),
#     StructField(
#         "interaction_type",
#         StringType(),
#         True,
#     ),
#     StructField(
#         "watch_time_sec",
#         IntegerType(),
#         True,
#     ),
#     StructField(
#         "device_type",
#         StringType(),
#         True,
#     ),
# ])


# with open(KAFKA_CA_CERT, "r") as file:
#     ca_pem_content = file.read()

# with open(KAFKA_SERVICE_CERT, "r") as file:
#     service_cert_content = file.read()

# with open(KAFKA_SERVICE_KEY, "r") as file:
#     service_key_content = file.read()


# df_kafka_raw = (
#     spark.readStream
#     .format("kafka")
#     .option(
#         "kafka.bootstrap.servers",
#         KAFKA_SERVICE_URL,
#     )
#     .option(
#         "subscribe",
#         TOPIC_NAME,
#     )
#     .option(
#         "startingOffsets",
#         "earliest",
#     )
#     .option(
#         "kafka.security.protocol",
#         "SSL",
#     )
#     .option(
#         "kafka.ssl.truststore.type",
#         "PEM",
#     )
#     .option(
#         "kafka.ssl.truststore.certificates",
#         ca_pem_content,
#     )
#     .option(
#         "kafka.ssl.endpoint.identification.algorithm",
#         "",
#     )
#     .option(
#         "kafka.ssl.keystore.type",
#         "PEM",
#     )
#     .option(
#         "kafka.ssl.keystore.certificate.chain",
#         service_cert_content,
#     )
#     .option(
#         "kafka.ssl.keystore.key",
#         service_key_content,
#     )
#     .load()
# )


# df_events = (
#     df_kafka_raw
#     .selectExpr(
#         "CAST(value AS STRING) AS json_value"
#     )
#     .select(
#         from_json(
#             col("json_value"),
#             json_schema,
#         ).alias("data")
#     )
#     .select("data.*")
# )


# query = (
#     df_events.writeStream
#     .format("parquet")
#     .outputMode("append")
#     .option(
#         "path",
#         BRONZE_PATH,
#     )
#     .option(
#         "checkpointLocation",
#         CHECKPOINT_PATH,
#     )
#     .trigger(
#         processingTime="10 seconds"
#     )
#     .start()
# )


# query.awaitTermination()

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))

from datetime import datetime
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from scripts.storage.config.paths import get_path_builder
from scripts.storage.config.spark import get_spark_session

from config import (
    KAFKA_SERVICE_URL,
    KAFKA_SERVICE_CERT,
    KAFKA_SERVICE_KEY,
    KAFKA_CA_CERT,
)

TOPIC_NAME = "video_interactions"
today = datetime.now().strftime("%Y-%m-%d")

# نفس طريقة postgres بالضبط
spark = get_spark_session(f"KafkaToMinIO_Bronze_{today}", env="development")
builder = get_path_builder()

BRONZE_PATH = builder.bronze_kafka_streaming(ingestion_date=today)
CHECKPOINT_PATH = builder.bronze_kafka_checkpoint()

print(f"Running for ingestion_date={today}")
print(f"Bronze bucket: {builder.bronze_bucket}")
print(f"Writing Kafka topic {TOPIC_NAME} -> {BRONZE_PATH}")

json_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("interaction_type", StringType(), True),
    StructField("watch_time_sec", IntegerType(), True),
    StructField("device_type", StringType(), True),
])

with open(KAFKA_CA_CERT, "r") as f:
    ca_pem_content = f.read()
with open(KAFKA_SERVICE_CERT, "r") as f:
    service_cert_content = f.read()
with open(KAFKA_SERVICE_KEY, "r") as f:
    service_key_content = f.read()

df_kafka_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVICE_URL)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.type", "PEM")
    .option("kafka.ssl.truststore.certificates", ca_pem_content)
    .option("kafka.ssl.endpoint.identification.algorithm", "")
    .option("kafka.ssl.keystore.type", "PEM")
    .option("kafka.ssl.keystore.certificate.chain", service_cert_content)
    .option("kafka.ssl.keystore.key", service_key_content)
    .load()
)

df_events = (
    df_kafka_raw.selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), json_schema).alias("data"))
    .select("data.*")
)

query = (
    df_events.writeStream.format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()