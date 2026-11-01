# from pymongo import MongoClient
# from scripts.generation.video_metadata_mongo import generate_video_metadata
# from config import DBM_HOST, DBM_PORT

# BATCH_SIZE = 5000

# def load_to_mongo():
#     client = MongoClient(host=DBM_HOST, port=DBM_PORT)
#     db = client['platform_db']
#     videos_collection = db['videos']
    
#     try:
#         client.admin.command('ping')
#         print(f"connect: platform_db.videos")
        
#         videos_generator = generate_video_metadata(n=50000)
        
#         batch = []
#         count = 0

#         for video in videos_generator:
#             batch.append(video)
#             count += 1

#             if len(batch) >= BATCH_SIZE:
#                 videos_collection.insert_many(batch)
#                 print(f"{count} video...")
#                 batch = []

#         if batch:
#             videos_collection.insert_many(batch)

#         print(f"f: {count} video")

#     except Exception as e:
#         print(f" Erro in batch {count//BATCH_SIZE}:{e}")
#     finally:
#         client.close()

# if __name__ == "__main__":
#     load_to_mongo()

# from pymongo import MongoClient

# from config import DBM_HOST, DBM_PORT
# from scripts.simulation.clock import BusinessClock
# from scripts.simulation.random_provider import RandomProvider
# from scripts.simulation.video_simulation import VideoSimulation


# TOTAL_VIDEOS = 50_000
# BATCH_SIZE = 5_000


# def load_videos(
#     *,
#     clock: BusinessClock,
#     random_provider: RandomProvider,
#     total_videos: int = TOTAL_VIDEOS,
#     batch_size: int = BATCH_SIZE,
# ) -> list[str]:

#     client = MongoClient(
#         host=DBM_HOST,
#         port=DBM_PORT,
#     )

#     db = client["platform_db"]
#     collection = db["videos"]

#     simulation = VideoSimulation(
#         clock=clock,
#         random_provider=random_provider,
#     )

#     count = 0
#     batch = []
#     video_ids = []

#     try:
#         client.admin.command("ping")

#         print("Connected: platform_db.videos")

#         for video in simulation.generate_videos(
#             total_videos
#         ):
#             batch.append(video)
#             video_ids.append(
#                 video["video_id"]
#             )

#             count += 1

#             if len(batch) >= batch_size:

#                 collection.insert_many(batch)

#                 print(
#                     f"Inserted {count} videos..."
#                 )

#                 batch.clear()

#         if batch:
#             collection.insert_many(batch)

#         print(
#             f"Completed: {count} videos inserted"
#         )

#         return video_ids

#     except Exception as e:

#         print(
#             f"Mongo load error after "
#             f"{count} videos: {e}"
#         )

#         raise

#     finally:

#         client.close()

#         print(
#             "Mongo connection closed"
#         )

from pymongo import MongoClient

from config import DBM_HOST, DBM_PORT


BATCH_SIZE = 5_000


def load_videos(
    *,
    videos,
    batch_size: int = BATCH_SIZE,
) -> list[str]:

    client = MongoClient(
        host=DBM_HOST,
        port=DBM_PORT,
    )

    db = client["platform_db"]
    collection = db["videos"]

    count = 0
    batch = []
    video_ids = []

    try:
        client.admin.command("ping")

        print("Connected: platform_db.videos")

        for video in videos:

            batch.append(video)

            video_ids.append(
                video["video_id"]
            )

            count += 1

            if len(batch) >= batch_size:

                collection.insert_many(batch)

                print(
                    f"Inserted {count} videos..."
                )

                batch.clear()

        if batch:
            collection.insert_many(batch)

        print(
            f"Completed: {count} videos inserted"
        )

        return video_ids

    except Exception as e:

        print(
            f"Mongo load error after "
            f"{count} videos: {e}"
        )

        raise

    finally:

        client.close()

        print(
            "Mongo connection closed"
        )