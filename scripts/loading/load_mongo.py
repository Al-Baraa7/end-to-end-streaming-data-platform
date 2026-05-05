from pymongo import MongoClient
from generation.video_metadata_mongo import generate_video_metadata
from config import DBM_HOST, DBM_PORT

BATCH_SIZE = 5000

def load_to_mongo():
    client = MongoClient(host=DBM_HOST, port=DBM_PORT)
    db = client['platform_db']
    videos_collection = db['videos']
    
    try:
        client.admin.command('ping')
        print(f"connect: platform_db.videos")
        
        videos_generator = generate_video_metadata(n=50000)
        
        batch = []
        count = 0

        for video in videos_generator:
            batch.append(video)
            count += 1

            if len(batch) >= BATCH_SIZE:
                videos_collection.insert_many(batch)
                print(f"{count} video...")
                batch = []

        if batch:
            videos_collection.insert_many(batch)

        print(f"f: {count} video")

    except Exception as e:
        print(f" Erro in batch {count//BATCH_SIZE}:{e}")
    finally:
        client.close()

if __name__ == "__main__":
    load_to_mongo()