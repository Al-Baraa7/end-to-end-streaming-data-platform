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


# simulation volume (optional) ---------------------------
# Dynamic volumetric parameters for the data generator.
# CLI flags in scripts/simulation/run_simulation.py override these per run. The
# default policy is the laptop-friendly 1,000 users / 500 videos under a
# 50,000-event ceiling; the former 10k/10k full-scale dataset is still available
# explicitly, e.g.
#   py -3 -m scripts.simulation.run_simulation --users 10000 --videos 10000 --events 10000
# and every knob can also be pinned via env vars without touching code.


def _env_int(name, default):
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


# ---- Volume policy (R13) -------------------------------------------------
# Default dataset: 1 000 users / 500 videos, under a 50 000-event CEILING.
# `SIM_KAFKA_EVENT_CEILING` is a CEILING, not a quota: the expected volume is
# DERIVED from the viewing-session model below and may legitimately land under
# it (the generator reports target vs. realized instead of rescaling to fit).
SIM_USER_COUNT = _env_int("SIM_USER_COUNT", 1_000)
SIM_VIDEO_COUNT = _env_int("SIM_VIDEO_COUNT", 500)
SIM_KAFKA_EVENT_CEILING = _env_int("SIM_KAFKA_EVENT_CEILING", 50_000)
SIM_MONGO_BATCH_SIZE = _env_int("SIM_MONGO_BATCH_SIZE", 10)

# ---- Viewing-session model (R13) ----------------------------------------
# Declared FIRST; the event budget is derived from it, never the reverse:
#     expected_events = users x sessions/user/yr x events/session
# At the default volumes: 1 000 x 12 x 4 = 48 000 expected events, which sits
# under the 50 000 ceiling. That headroom is the point: the ceiling stays a
# safety limit (D4) and per-video Poisson lambdas are never rescaled to hit it.
SIM_SESSIONS_PER_USER_PER_YEAR = _env_int("SIM_SESSIONS_PER_USER_PER_YEAR", 12)
SIM_EVENTS_PER_SESSION = _env_int("SIM_EVENTS_PER_SESSION", 4)

# Optional floors applied to final video stats after event coupling.
# 0 = stats are an exact count of generated events (no phantom views).
SIM_MIN_STATS_VIEWS = _env_int("SIM_MIN_STATS_VIEWS", 0)
SIM_MIN_STATS_LIKES = _env_int("SIM_MIN_STATS_LIKES", 0)

