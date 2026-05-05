import uuid
import random
import json
from datetime import datetime, timedelta, timezone
from faker import Faker
import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

faker = Faker()


conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
)
cur = conn.cursor()
cur.execute("SELECT user_id FROM users;")
USER_IDS = [row[0] for row in cur.fetchall()]
cur.close()
conn.close()

if not USER_IDS:
    raise Exception("impty")


RSL_TEAMS = [
    'Al_Hilal', 'Al_Nassr', 'Al_Ittihad', 'Al_Ahli', 'Al_Ettifaq', 'Al_Taawoun',
    'Al_Fateh', 'Al_Fayha', 'Al_Khaleej', 'Al_Okhdood', 'Al_Raed', 'Al_Riyadh',
    'Al_Shabab', 'Al_Wehda', 'Damac', 'Al_Qadsiah', 'Al_Orobah', 'Al_Kholood'
]
KINGS_CUP_TEAMS = RSL_TEAMS + [
    'Al_Jabalain', 'Al_Bukiryah', 'Al_Jandal', 'Al_Najma', 'Al_Ain', 'Al_Safa',
    'Al_Adalah', 'Al_Batin', 'Ohud', 'Jeddah', 'Al_Jubail', 'Hajer', 'Al_Shoalah', 'Al_Faisaly'
]
SUPER_CUP_TEAMS = ['Al_Hilal', 'Al_Nassr', 'Al_Ittihad', 'Al_Ahli']
COMPETITIONS = {'RSL': RSL_TEAMS, 'Kings_Cup': KINGS_CUP_TEAMS, 'Super_Cup': SUPER_CUP_TEAMS}

CONTENT_TYPES = ['live', 'highlight', 'replay']
LANGUAGES = ['ar', 'en']
RESOLUTIONS = ['4k', '1080p', '720p', '480p']
CODECS = ['H.264', 'H.265']
STATUS = ['streaming', 'ended', 'archived']
TAGS = ['football', 'highlights', 'live', 'RSL', 'Kings_Cup', 'Super_Cup', 'goals', 'replay', 'VAR']


def generate_video_metadata(n):
    for _ in range(n):
        comp_name = random.choice(list(COMPETITIONS.keys()))
        team1, team2 = random.sample(COMPETITIONS[comp_name], 2)
        status = random.choice(STATUS)
        created_at = faker.date_time_between(start_date='-30d', end_date='now', tzinfo=timezone.utc)

        if status in ['ended', 'archived']:
            last_updated = created_at + timedelta(minutes=random.randint(5, 150))
            duration = random.randint(300, 7200)
        else:
            last_updated = created_at
            duration = None

        
        description = faker.text(max_nb_chars=150) if random.random() > 0.08 else None
        match_id = f"match_{random.randint(1000, 9999)}" if random.random() > 0.05 else None
        title = f"{team1} vs {team2}" if random.random() > 0.03 else ""

        if status!= 'streaming' and random.random() < 0.05:
            duration = random.choice([-1, 0, 999999])

        base_tags = random.sample(TAGS, k=random.randint(2, 4))
        if random.random() < 0.1:
            base_tags = base_tags + random.sample(base_tags, k=1)

        languages = random.sample(LANGUAGES, k=random.randint(1, 2)) if random.random() > 0.04 else []

        video = {
            'video_id': str(uuid.uuid4()),
            'user_id': random.choice(USER_IDS),
            'content_type': random.choice(CONTENT_TYPES),
            'match_id': match_id,
            'competition': {'id': comp_name, 'name': comp_name.replace('_', ' '), 'teams': [team1, team2]},
            'title': title,
            'description': description,
            'languages': languages,
            'tags': base_tags,
            'duration_sec': duration,
            'technical': {
                'max_resolution': random.choice(RESOLUTIONS) if random.random() > 0.02 else None,
                'codec': random.choice(CODECS)
            },
            'status': status,
            'stats': {
                'views': random.randint(0, 500000) if random.random() > 0.05 else None,
                'likes': random.randint(0, 50000)
            },
            'created_at': created_at,
            'last_updated': last_updated
        }
        yield video

# 
if __name__ == "__main__":
    TOTAL_DOCS = 50000
    print(f"{TOTAL_DOCS} log...")

    for doc in generate_video_metadata(n=TOTAL_DOCS):
        print(json.dumps(doc, default=str))

    print("f")