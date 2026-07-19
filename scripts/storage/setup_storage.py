# from google.cloud import storage
# from google.auth.credentials import AnonymousCredentials

# client = storage.Client(
#     project="streaming-data-platform",
#     credentials=AnonymousCredentials(),
#     client_options={
#         "api_endpoint":"http://localhost:4443"
#     }
# )

# # bucket_name = "end-to-end-streaming-data-platform"
# bucket_name_silver = "end-to-end-streaming-data-platform-silver"
# # bucket_name_gold = "end-to-end-streaming-data-platform-gold"
   
# try:
#     bucket=client.create_bucket(bucket_name_silver)
#     print(f"Bucket created: {bucket_name_silver}")

# except Exception as e:
#     bucket= client.bucket(bucket_name_silver)
#     print(f"ERROR: {e}")

import boto3
from botocore.client import Config

minio_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000', 
    aws_access_key_id='minio',        
    aws_secret_access_key='minio123',    
    config=Config(signature_version='s3v4')
)


buckets = [
    "end-to-end-streaming-data-platform-bronze",
    "end-to-end-streaming-data-platform-silver",
    "end-to-end-streaming-data-platform-gold"
]


for bucket_name in buckets:
    try:
        minio_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket created: {bucket_name}")
        
    except Exception as e:
        
        if "BucketAlreadyOwnsToYou" in str(e) or "BucketAlreadyExists" in str(e):
            print(f"Bucket already exists: {bucket_name}")
        else:
            print(f"ERROR creating {bucket_name}: {e}")
        