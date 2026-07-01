from google.cloud import storage
from google.auth.credentials import AnonymousCredentials

client = storage.Client(
    project="streaming-data-platform",
    credentials=AnonymousCredentials(),
    client_options={
        "api_endpoint":"http://localhost:4443"
    }
)

# bucket_name = "end-to-end-streaming-data-platform"
bucket_name_silver = "end-to-end-streaming-data-platform-silver"
# bucket_name_gold = "end-to-end-streaming-data-platform-gold"
   
try:
    bucket=client.create_bucket(bucket_name_silver)
    print(f"Bucket created: {bucket_name_silver}")

except Exception as e:
    bucket= client.bucket(bucket_name_silver)
    print(f"ERROR: {e}") 