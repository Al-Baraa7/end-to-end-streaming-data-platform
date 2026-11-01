# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

# today = datetime.now().strftime("%Y-%m-%d-%h")

# spark = SparkSession.builder \
#     .appName("PostgresToMinIO_Bronze") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "minio") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()

# jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
# connection_properties = {
#     "user": DB_USER,
#     "password": DB_PASSWORD,
#     "driver": "org.postgresql.Driver"
# }

# def convert_uuid_columns(df, uuid_columns):
#     for column in uuid_columns:
#         if column in df.columns:
#             df = df.withColumn(column, col(column).cast("string"))
#     return df

# base_s3_path = f"s3a://end-to-end-streaming-data-platform-bronze/postgres/ingestion_date={today}"

# df_user = spark.read.jdbc(url=jdbc_url, table="users", properties=connection_properties)
# df_user = convert_uuid_columns(df_user, ["user_id"])
# df_user.write.mode("overwrite").parquet(f"{base_s3_path}/users.parquet")

# df_sub = spark.read.jdbc(url=jdbc_url, table="subscription", properties=connection_properties)
# df_sub = convert_uuid_columns(df_sub, ["subscription_id", "user_id"])
# df_sub.write.mode("overwrite").parquet(f"{base_s3_path}/subscription.parquet")

# df_pay = spark.read.jdbc(url=jdbc_url, table="payments", properties=connection_properties)
# df_pay = convert_uuid_columns(df_pay, ["payment_id", "subscription_id"])
# df_pay.write.mode("overwrite").parquet(f"{base_s3_path}/payments.parquet")

# df_plan_pricing = spark.read.jdbc(url=jdbc_url, table="plan_pricing", properties=connection_properties)
# df_plan_pricing.write.mode("overwrite").parquet(f"{base_s3_path}/plan_pricing.parquet")

# spark.stop()

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))

from datetime import datetime
from pyspark.sql.functions import col

from scripts.storage.config.paths import get_path_builder, SourceSystem
from scripts.storage.config.spark import get_spark_session
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT


today = datetime.now().strftime("%Y-%m-%d")

spark = get_spark_session(f"PostgresToMinIO_Bronze_{today}", env="development")

jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
connection_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def convert_uuid_columns(df, uuid_columns):
    for column in uuid_columns:
        if column in df.columns:
            df = df.withColumn(column, col(column).cast("string"))
    return df

builder = get_path_builder()

print(f"Running for ingestion_date={today}")
print(f"Bronze bucket: {builder.bronze_bucket}")

tables_config = [
    ("users", ["user_id"]),
    ("subscription", ["subscription_id", "user_id"]),
    ("payments", ["payment_id", "subscription_id"]),
    ("plan_pricing", [])
]

for table, uuid_cols in tables_config:
    path = builder.bronze_partitioned(SourceSystem.POSTGRES, today, table)
    print(f"Reading {table} -> Writing to {path}")
    
    df = spark.read.jdbc(url=jdbc_url, table=table, properties=connection_properties)
    df = convert_uuid_columns(df, uuid_cols)
    df.write.mode("overwrite").parquet(path)

spark.stop()
print("Done.")