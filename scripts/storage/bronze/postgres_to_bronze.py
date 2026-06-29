import pandas as pd
from io import BytesIO
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
from sqlalchemy import create_engine
from datetime import datetime
from config import DB_HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT

today = datetime.now().strftime("%Y-%m-%d-%h")
client = storage.Client(
    project="streaming-data-platform",
    credentials=AnonymousCredentials(),
    client_options={
        "api_endpoint":"http://localhost:4443"
    }
)
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
#--------------------------------------
def convert_uuid_columns(df,uuid_columns):
    for col in uuid_columns:
        df[col]=df[col].astype(str)
    return df
#-----------------------------
buffer = BytesIO()
df_user=pd.read_sql("SELECT * FROM users",engine)
df_user=convert_uuid_columns(df_user,["user_id"])
df_user.to_parquet(buffer,index=False)
buffer.seek(0)
blob = client.bucket(f"end-to-end-streaming-data-platform").blob(
        f"bronze/postgres/ingestion_date={today}/users.parquet")
blob.upload_from_file(buffer, content_type="application/octet-stream")
#-----------------------------------------------
buffer = BytesIO()
df_sub=pd.read_sql("SELECT * FROM subscription",engine)
df_sub=convert_uuid_columns(df_sub,["subscription_id","user_id"])
df_sub.to_parquet(buffer,index=False)
buffer.seek(0)
blob = client.bucket(f"end-to-end-streaming-data-platform").blob(
        f"bronze/postgres/ingestion_date{today}/subscription.parquet")
blob.upload_from_file(buffer,content_type="application/octet-stream")
#------------------------------------------------
buffer = BytesIO()
df_pay=pd.read_sql("SELECT * FROM payments",engine)
df_pay=convert_uuid_columns(df_pay,["payment_id","subscription_id"])
df_pay.to_parquet(buffer,index=False)
buffer.seek(0)
blob = client.bucket(f"end-to-end-streaming-data-platform").blob(
        f"bronze/postgres/ingestion_date{today}/payments.parquet")
blob.upload_from_file(buffer,content_type="application/octet-stream") 