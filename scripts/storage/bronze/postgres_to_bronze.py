# import pandas as pd
# from io import BytesIO
# from google.cloud import storage
# from google.auth.credentials import AnonymousCredentials
# from sqlalchemy import create_engine
# from datetime import datetime
# from config import DB_HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT

# today = datetime.now().strftime("%Y-%m-%d-%h")
# client = storage.Client(
#     project="streaming-data-platform",
#     credentials=AnonymousCredentials(),
#     client_options={
#         "api_endpoint":"http://localhost:4443"
#     }
# )
# engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
# #--------------------------------------
# def convert_uuid_columns(df,uuid_columns):
#     for col in uuid_columns:
#         df[col]=df[col].astype(str)
#     return df
# #-----------------------------
# buffer = BytesIO()
# df_user=pd.read_sql("SELECT * FROM users",engine)
# df_user=convert_uuid_columns(df_user,["user_id"])
# df_user.to_parquet(buffer,index=False)
# buffer.seek(0)
# blob = client.bucket(f"end-to-end-streaming-data-platform").blob(
#         f"bronze/postgres/ingestion_date={today}/users.parquet")
# blob.upload_from_file(buffer, content_type="application/octet-stream")
# #-----------------------------------------------
# buffer = BytesIO()
# df_sub=pd.read_sql("SELECT * FROM subscription",engine)
# df_sub=convert_uuid_columns(df_sub,["subscription_id","user_id"])
# df_sub.to_parquet(buffer,index=False)
# buffer.seek(0)
# blob = client.bucket(f"end-to-end-streaming-data-platform").blob(
#         f"bronze/postgres/ingestion_date{today}/subscription.parquet")
# blob.upload_from_file(buffer,content_type="application/octet-stream")
# #------------------------------------------------
# buffer = BytesIO()
# df_pay=pd.read_sql("SELECT * FROM payments",engine)
# df_pay=convert_uuid_columns(df_pay,["payment_id","subscription_id"])
# df_pay.to_parquet(buffer,index=False)
# buffer.seek(0)
# blob = client.bucket(f"end-to-end-streaming-data-platform").blob(
#         f"bronze/postgres/ingestion_date{today}/payments.parquet")
# blob.upload_from_file(buffer,content_type="application/octet-stream")


# import pandas as pd
# from io import BytesIO
# import boto3
# from botocore.client import Config
# from sqlalchemy import create_engine
# from datetime import datetime
# from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

# today = datetime.now().strftime("%Y-%m-%d-%h")

# minio_client = boto3.client(
#     's3',
#     endpoint_url='http://localhost:9000',
#     aws_access_key_id='minio',            
#     aws_secret_access_key='minio123',     
#     config=Config(signature_version='s3v4')
# )

# engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
# #--------------------------------------
# def convert_uuid_columns(df, uuid_columns):
#     for col in uuid_columns:
#         df[col] = df[col].astype(str)
#     return df
# #-----------------------------
# buffer = BytesIO()
# df_user = pd.read_sql("SELECT * FROM users", engine)
# df_user = convert_uuid_columns(df_user, ["user_id"])
# df_user.to_parquet(buffer, index=False)
# buffer.seek(0)

# minio_client.upload_fileobj(
#     buffer, 
#     Bucket="end-to-end-streaming-data-platform-bronze", 
#     Key=f"bronze/postgres/ingestion_date={today}/users.parquet"
# )
# #-----------------------------------------------
# buffer = BytesIO()
# df_sub = pd.read_sql("SELECT * FROM subscription", engine)
# df_sub = convert_uuid_columns(df_sub, ["subscription_id", "user_id"])
# df_sub.to_parquet(buffer, index=False)
# buffer.seek(0)


# minio_client.upload_fileobj(
#     buffer, 
#     Bucket="end-to-end-streaming-data-platform-bronze", 
#     Key=f"bronze/postgres/ingestion_date={today}/subscription.parquet"
# )
# #------------------------------------------------
# buffer = BytesIO()
# df_pay = pd.read_sql("SELECT * FROM payments", engine)
# df_pay = convert_uuid_columns(df_pay, ["payment_id", "subscription_id"])
# df_pay.to_parquet(buffer, index=False)
# buffer.seek(0)


# minio_client.upload_fileobj(
#     buffer, 
#     Bucket="end-to-end-streaming-data-platform-bronze", 
#     Key=f"bronze/postgres/ingestion_date={today}/payments.parquet"
# )

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

today = datetime.now().strftime("%Y-%m-%d-%h")

spark = SparkSession.builder \
    .appName("PostgresToMinIO_Bronze") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

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

base_s3_path = f"s3a://end-to-end-streaming-data-platform-bronze/postgres/ingestion_date={today}"

df_user = spark.read.jdbc(url=jdbc_url, table="users", properties=connection_properties)
df_user = convert_uuid_columns(df_user, ["user_id"])
df_user.write.mode("overwrite").parquet(f"{base_s3_path}/users.parquet")

df_sub = spark.read.jdbc(url=jdbc_url, table="subscription", properties=connection_properties)
df_sub = convert_uuid_columns(df_sub, ["subscription_id", "user_id"])
df_sub.write.mode("overwrite").parquet(f"{base_s3_path}/subscription.parquet")

df_pay = spark.read.jdbc(url=jdbc_url, table="payments", properties=connection_properties)
df_pay = convert_uuid_columns(df_pay, ["payment_id", "subscription_id"])
df_pay.write.mode("overwrite").parquet(f"{base_s3_path}/payments.parquet")

spark.stop()