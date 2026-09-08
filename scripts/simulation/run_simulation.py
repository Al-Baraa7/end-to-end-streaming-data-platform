from datetime import datetime, timezone

from scripts.simulation.clock import BusinessClock
from scripts.simulation.random_provider import RandomProvider
from scripts.simulation.postgres.postgres_simulation import PostgresSimulation
from scripts.simulation.dirty_data.postgres_dirty_injection import (
    PostgresDirtyDataInjector,
)
from scripts.simulation.mongo.video_simulation import VideoSimulation
from scripts.simulation.dirty_data.mongo_dirty_injection import MongoDirtyDataInjector
from scripts.simulation.dirty_data.kafka_dirty_injection import KafkaDirtyDataInjector
from scripts.simulation.kafka.interaction_factory import InteractionFactory

from scripts.loading.load_postgres import load_postgres
from scripts.loading.load_mongo import load_videos

from scripts.simulation.kafka.video_events_producer import run_producer


# ============================================================
# CONFIG
# ============================================================

START_TIME = datetime(
    2026,
    1,
    1,
    0,
    0,
    tzinfo=timezone.utc,
)

RANDOM_SEED = 42

USER_COUNT = 10_000
VIDEO_COUNT = 10_000
KAFKA_EVENT_COUNT = 30_000


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("STARTING NEW DATA PLATFORM PIPELINE")
    print("=" * 70)

    # --------------------------------------------------------
    # 1. Shared simulation infrastructure
    # --------------------------------------------------------

    clock = BusinessClock(
        START_TIME
    )

    random_provider = RandomProvider(
        seed=RANDOM_SEED
    )

    print("\n[1] Simulation initialized")
    print("Clock:", clock.now())
    print("Seed:", RANDOM_SEED)

    # --------------------------------------------------------
    # 2. PostgreSQL simulation
    # --------------------------------------------------------

    postgres_simulation = PostgresSimulation(
        clock=clock,
        random_provider=random_provider,
    )

    # --------------------------------------------------------
    # 3. Generate Videos FIRST at START_TIME
    # --------------------------------------------------------

    print("\n[2] Generating MongoDB videos...")

    video_simulation = VideoSimulation(
        clock=clock,
        random_provider=random_provider,
    )

    videos = list(video_simulation.generate_videos(
        count=VIDEO_COUNT
    ))

    video_ids = [v["video_id"] for v in videos]

    mongo_dirty_injector = MongoDirtyDataInjector()

    videos = list(mongo_dirty_injector.inject(
        videos=videos
    ))

    print("\n[3] Loading MongoDB...")

    video_ids = load_videos(
        videos=videos,
        batch_size=10,
    )

    print(
        f"[3] MongoDB loading completed: "
        f"{len(video_ids)} videos"
    )

    # --------------------------------------------------------
    # 4. Users + Subscriptions + Payments + Interactions
    # --------------------------------------------------------

    users = []
    subscriptions = []
    payments = []
    all_events = []

    interaction_factory = InteractionFactory(
        clock=clock,
        random_provider=random_provider,
    )

    for user_idx in range(USER_COUNT):
        user = postgres_simulation.generate_user()
        users.append(user)

        user_subscriptions = []
        subscription_count = random_provider.randint(
            postgres_simulation.MIN_SUBSCRIPTIONS_PER_USER,
            postgres_simulation.MAX_SUBSCRIPTIONS_PER_USER,
        )

        for _ in range(subscription_count):
            subscription = postgres_simulation.generate_subscription(
                user_id=user.user_id
            )
            user_subscriptions.append(subscription)
            subscriptions.append(subscription)

            payment_chain = postgres_simulation.generate_payment_chain(
                subscription=subscription,
                max_attempts=3,
            )
            payments.extend(payment_chain)

        # ----------------------------------------------------
        # 5. Generate exactly 3 interactions for this user
        # ----------------------------------------------------
        
        # Filter valid subscriptions at current clock time
        valid_subscriptions = [
            s for s in user_subscriptions
            if clock.now() <= s.end_date
        ]

        if not valid_subscriptions:
            raise RuntimeError(
                f"User {user.user_id} has no valid subscription at simulation time {clock.now()}"
            )

        for _ in range(3):  # exactly 3 events per user
            # Choose a valid subscription
            subscription = random_provider.choice(valid_subscriptions)
            
            # Choose a video
            video = random_provider.choice(videos)
            
            # Use current simulation time as event timestamp
            event_ts = clock.now()
            
            # Verify temporal validity
            if not (
                event_ts >= user.created_at
                and event_ts >= subscription.start_date
                and event_ts <= subscription.end_date
                and event_ts >= video["created_at"]
            ):
                raise RuntimeError(
                    f"Temporal validity check failed for user {user.user_id}: "
                    f"event_ts={event_ts}, user.created_at={user.created_at}, "
                    f"sub.start={subscription.start_date}, sub.end={subscription.end_date}, "
                    f"video.created_at={video['created_at']}"
                )
            
            event = interaction_factory.create(
                user_id=user.user_id,
                video_id=video["video_id"],
                video_duration=video["duration_sec"],
                event_timestamp=event_ts,
            )
            all_events.append(event)
            
            # Advance clock 5-120 seconds between events
            clock.advance(
                seconds=random_provider.randint(5, 120)
            )

        # Gap to next user
        clock.advance(
            minutes=random_provider.randint(30, 60)
        )

    print(
        f"\n[4] Users generated: {len(users)}"
    )
    print(
        f"[5] Subscriptions generated: {len(subscriptions)}"
    )
    print(
        f"[6] Payments generated: {len(payments)}"
    )
  

    # Dirty injection for Postgres
    dirty_injector = PostgresDirtyDataInjector()

    users, subscriptions, payments = dirty_injector.inject(
        users=users,
        subscriptions=subscriptions,
        payments=payments,
    )

    # --------------------------------------------------------
    # 6. Load PostgreSQL
    # --------------------------------------------------------

    print("\n[7] Loading PostgreSQL...")

    load_postgres(
        users=users,
        subscriptions=subscriptions,
        payments=payments,
    )

    print(
        "[7] PostgreSQL loading completed"
    )

    # --------------------------------------------------------
    # 7. Kafka - single producer with pre-generated events
    # --------------------------------------------------------

    print("\n[8] Starting Kafka producer...")

    kafka_dirty_injector = KafkaDirtyDataInjector()

    run_producer(
        clock=clock,
        random_provider=random_provider,
        user_ids=[u.user_id for u in users],
        video_ids=video_ids,
        event_count=KAFKA_EVENT_COUNT,
        dirty_injector=kafka_dirty_injector,
        events=all_events,  # NEW: pass pre-generated events
    )

    print(
        "[8] Kafka producer completed"
    )

    # --------------------------------------------------------
    # 8. Final report + assertion
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("SOURCE PIPELINE COMPLETED")
    print("=" * 70)

    print(
        "Users:",
        len(users),
    )

    print(
        "Subscriptions:",
        len(subscriptions),
    )

    print(
        "Payments:",
        len(payments),
    )

    print(
         "Videos:",
         len(video_ids),
    )

    print(
         "Kafka events:",
         len(all_events),
    )

    print(
        "Final BusinessClock:",
        clock.now(),
    )

    print("=" * 70)

    # ASSERT exactly KAFKA_EVENT_COUNT
    assert len(all_events) == KAFKA_EVENT_COUNT, (
        f"Expected {KAFKA_EVENT_COUNT} events, generated {len(all_events)}"
    )


if __name__ == "__main__":
    main()