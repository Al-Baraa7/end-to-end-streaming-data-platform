

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

            video.pop("latent_popularity", None)

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