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
bucket_name_gold = "end-to-end-streaming-data-platform-gold"
for name in [bucket_name_silver,bucket_name_gold]:   
    try:
        bucket=client.create_bucket(name)
        print(f"Bucket created: {name}")

    except Exception as e:
        bucket= client.bucket(name)
        print(f"ERROR: {e}") 