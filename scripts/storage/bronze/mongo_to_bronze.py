import pandas as pd
from io import BytesIO
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
from pymongo import MongoClient
from datetime import datetime
from config import DBM_HOST,DBM_PORT

today= datetime.now().strftime("%Y-%m-%d-%h")
gcs_client = storage.Client(
    project="end-to-end-streaming-data-platform",
    credentials=AnonymousCredentials(),
    client_options={
        "api_endpoint" : "http://localhost:4443"
    }
)

mongo_client = MongoClient(host=DBM_HOST,port=DBM_PORT)
def convert_uuid_columns(df,uuid_columns):
    for col in uuid_columns:
        df[col]=df[col].astype(str)
    return df

buffer = BytesIO()
db= mongo_client["platform_db"]
collection = db["videos"]
docs = list(collection.find())

df_video = pd.DataFrame(docs)
df_video =convert_uuid_columns(df_video,["_id","video_id","user_id"])
df_video.to_parquet(buffer,index=False)
buffer.seek(0)
#------------------------
blob = gcs_client.bucket(f"end-to-end-streaming-data-platform").blob(
    f"bronze/mongo/ingestion_data={today}/videos.parquet"
)
blob.upload_from_file(buffer,content_type="application/octet-stream")