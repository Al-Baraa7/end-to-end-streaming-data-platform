from dotenv import load_dotenv
import os

load_dotenv()

# connect postgres

DB_HOST= os.getenv("DB_HOST")
DB_NAME= os.getenv("DB_NAME")
DB_USER= os.getenv("DB_USER")
DB_PASSWORD= os.getenv("DB_PASSWORD")
DB_PORT_RAW= os.getenv("DB_PORT")

if not all([DB_HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT_RAW]):
    raise ValueError("postgres config missing in env")
DB_PORT= int(DB_PORT_RAW)


# connect mongo----------------

DBM_HOST= os.getenv("DBM_HOST")
DBM_PORT_RAW= os.getenv("DBM_PORT")

if not all([DBM_HOST,DBM_PORT_RAW]):
    raise ValueError("mongo config missing in env")
DBM_PORT= int(DBM_PORT_RAW)

# connect kafka---------------------------

KAFKA_SERVICE_URL= os.getenv("SERVICE_URL")
KAFKA_SERVICE_CERT= os.getenv("SERVICE_CERT")
KAFKA_SERVICE_KEY= os.getenv("SERVICE_KEY")
KAFKA_CA_CERT= os.getenv("CA_CERTIFICATE")

if not all([KAFKA_SERVICE_URL,KAFKA_SERVICE_CERT,
            KAFKA_SERVICE_KEY,KAFKA_CA_CERT]):
    raise ValueError("kafka config missing in env")
