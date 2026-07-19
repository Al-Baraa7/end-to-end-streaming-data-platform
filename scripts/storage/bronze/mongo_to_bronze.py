# import pandas as pd
# from io import BytesIO
# from google.cloud import storage
# from google.auth.credentials import AnonymousCredentials
# from pymongo import MongoClient
# from datetime import datetime
# from config import DBM_HOST,DBM_PORT

# today= datetime.now().strftime("%Y-%m-%d-%h")
# gcs_client = storage.Client(
#     project="end-to-end-streaming-data-platform",
#     credentials=AnonymousCredentials(),
#     client_options={
#         "api_endpoint" : "http://localhost:4443"
#     }
# )

# mongo_client = MongoClient(host=DBM_HOST,port=DBM_PORT)
# def convert_uuid_columns(df,uuid_columns):
#     for col in uuid_columns:
#         df[col]=df[col].astype(str)
#     return df

# buffer = BytesIO()
# db= mongo_client["platform_db"]
# collection = db["videos"]
# docs = list(collection.find())

# df_video = pd.DataFrame(docs)
# df_video =convert_uuid_columns(df_video,["_id","video_id","user_id"])
# df_video.to_parquet(buffer,index=False)
# buffer.seek(0)
# #------------------------
# blob = gcs_client.bucket(f"end-to-end-streaming-data-platform").blob(
#     f"bronze/mongo/ingestion_data={today}/videos.parquet"
# )
# blob.upload_from_file(buffer,content_type="application/octet-stream")

# import pandas as pd
# from io import BytesIO
# import boto3
# from botocore.client import Config
# from pymongo import MongoClient
# from datetime import datetime
# from config import DBM_HOST, DBM_PORT

# today = datetime.now().strftime("%Y-%m-%d-%h")


# minio_client = boto3.client(
#     's3',
#     endpoint_url='http://localhost:9000',
#     aws_access_key_id='minio',            
#     aws_secret_access_key='minio123',     
#     config=Config(signature_version='s3v4')
# )

# mongo_client = MongoClient(host=DBM_HOST, port=DBM_PORT)

# def convert_uuid_columns(df, uuid_columns):
#     for col in uuid_columns:
#         df[col] = df[col].astype(str)
#     return df

# buffer = BytesIO()
# db = mongo_client["platform_db"]
# collection = db["videos"]
# docs = list(collection.find())

# df_video = pd.DataFrame(docs)
# df_video = convert_uuid_columns(df_video, ["_id", "video_id", "user_id"])
# df_video.to_parquet(buffer, index=False)
# buffer.seek(0)
# #------------------------


# minio_client.upload_fileobj(
#     buffer, 
#     Bucket="end-to-end-streaming-data-platform-bronze", 
#     Key=f"bronze/mongo/ingestion_data={today}/videos.parquet"
# )

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config import DBM_HOST, DBM_PORT

today = datetime.now().strftime("%Y-%m-%d-%h")

spark = SparkSession.builder \
    .appName("MongoToMinIO_Bronze") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

def convert_uuid_columns(df, uuid_columns):
    for column in uuid_columns:
        if column in df.columns:
            df = df.withColumn(column, col(column).cast("string"))
    return df

df_video = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.read.connection.uri", f"mongodb://{DBM_HOST}:{DBM_PORT}") \
    .option("spark.mongodb.read.database", "platform_db") \
    .option("spark.mongodb.read.collection", "videos") \
    .load()

df_video = convert_uuid_columns(df_video, ["_id", "video_id", "user_id"])

base_s3_path = f"s3a://end-to-end-streaming-data-platform-bronze/mongo/ingestion_data={today}"

df_video.write \
    .mode("overwrite") \
    .parquet(f"{base_s3_path}/videos.parquet")

spark.stop()