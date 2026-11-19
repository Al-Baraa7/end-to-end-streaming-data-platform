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
        