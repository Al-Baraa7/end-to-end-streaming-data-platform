from dotenv import load_dotenv
import os

load_dotenv()

# connect postgres
DB_HOST= os.getenv("DB_HOST")
DB_NAME= os.getenv("DB_NAME")
DB_USER= os.getenv("DB_USER")
DB_PASSWORD= os.getenv("DB_PASSWORD")
DB_PORT= int(os.getenv("DB_PORT"))

if not all([DB_HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT]):
    raise ValueError("x")

# connect mongo

DBM_HOST= os.getenv("DBM_HOST")
DBM_PORT= int(os.getenv("DBM_PORT"))

if not all([DBM_HOST,DBM_PORT]):
    raise ValueError("Z")