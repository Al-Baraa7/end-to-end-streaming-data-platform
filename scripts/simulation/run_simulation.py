from datetime import datetime, timedelta, timezone

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
from scripts.simulation.temporal_registry import TemporalRegistry


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

SIMULATION_END = datetime(
    2026,
    12,
    31,
    23,
    59,
    59,
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
        START_TIME,
        simulation_end=SIMULATION_END,
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
        simulation_start=START_TIME,
        simulation_end=SIMULATION_END,
    )

    videos = list(video_simulation.generate_videos(
        count=VIDEO_COUNT
    ))

    video_ids = [v["video_id"] for v in videos]

    # Extract live match schedule for event clustering
    live_matches = [
        {
            "match_id": v["match_id"],
            "live_start_at": v["live_start_at"],
            "live_end_at": v["live_end_at"],
            "video_id": v["video_id"],
        }
        for v in videos
        if v["content_type"] == "live" and v["live_start_at"] is not None
    ]

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
        f"{len(video_ids)} videos ({len(live_matches)} live matches)"
    )

    # --------------------------------------------------------
    # PHASE 1: Generate ALL Users with organic sign-up timing
    # --------------------------------------------------------

    print("\n[4] Generating users with organic growth timeline...")

    users = []
    user_subscriptions_map = {}  # user_id -> list of subscriptions

    for user_idx in range(USER_COUNT):
        user = postgres_simulation.generate_user()
        users.append(user)

        # Organic sign-up gap: 30 seconds to 45 minutes between users
        clock.advance(
            seconds=random_provider.randint(30, 2700)
        )

        # Progress logging
        if (user_idx + 1) % 1000 == 0:
            print(f"  Generated {user_idx + 1}/{USER_COUNT} users (clock: {clock.now()})")

    print(f"  Completed: {len(users)} users generated")
    print(f"  Timeline spans: {users[0].created_at} -> {users[-1].created_at}")

    # --------------------------------------------------------
    # PHASE 2: For each user, generate subscriptions & payments
    # at their signup time (no global clock jumps to sub end)
    # --------------------------------------------------------

    print("\n[5] Generating subscriptions & payments per user...")

    subscriptions = []
    payments = []

    for user_idx, user in enumerate(users):
        # Temporarily set clock to user's signup time for their lifecycle
        user_signup_time = user.created_at
        clock.advance_to(user_signup_time)

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

        user_subscriptions_map[user.user_id] = user_subscriptions

        if (user_idx + 1) % 1000 == 0:
            print(f"  Processed {user_idx + 1}/{len(users)} users for subscriptions")

    # Restore clock to end of user generation period
    clock.advance_to(users[-1].created_at)
    print(f"  Completed: {len(subscriptions)} subscriptions, {len(payments)} payments")

    # --------------------------------------------------------
    # PHASE 3: Generate Kafka events distributed across timeline
    # - Cluster events around live matches (peak traffic)
    # - Distribute remaining events across user lifetimes with diurnal patterns
    # --------------------------------------------------------

    print("\n[6] Generating Kafka watch events with realistic patterns...")

    interaction_factory = InteractionFactory(
        clock=clock,
        random_provider=random_provider,
    )

    # Initialize TemporalRegistry as Single Source of Truth for referential integrity
    registry = TemporalRegistry()
    registry.load_from_memory([s.to_dict() for s in subscriptions], videos)

    all_events = []

    # Build event generation plan: each user gets ~3 events distributed
    events_per_user = max(1, KAFKA_EVENT_COUNT // USER_COUNT)
    remaining_events = KAFKA_EVENT_COUNT

    for user_idx, user in enumerate(users):
        user_subs = user_subscriptions_map.get(user.user_id, [])
        if not user_subs:
            continue

        # How many events for this user?
        user_event_count = min(events_per_user, remaining_events)
        if remaining_events <= 0:
            break

        # Get valid subscriptions for this user
        valid_subs = [s for s in user_subs if s.end_date >= user.created_at]
        if not valid_subs:
            continue

        for _ in range(user_event_count):
            subscription = random_provider.choice(valid_subs)

            # Choose event timing strategy:
            # 60% chance: event during a live match (if any match overlaps subscription)
            # 40% chance: event during regular hours (diurnal pattern)

            use_live_match = (
                random_provider.random() < 0.60
                and live_matches
            )

            if use_live_match:
                # Pick a live match that overlaps with subscription
                overlapping_matches = [
                    m for m in live_matches
                    if m["live_start_at"] >= user.created_at
                    and m["live_start_at"] <= subscription.end_date
                ]
                if overlapping_matches:
                    match = random_provider.choice(overlapping_matches)
                    # Event during match: 0 to 120 min after match start
                    event_ts = match["live_start_at"] + timedelta(
                        minutes=random_provider.randint(0, min(120, int((match["live_end_at"] - match["live_start_at"]).total_seconds() / 60)))
                    )
                    video = next(v for v in videos if v["video_id"] == match["video_id"])
                else:
                    use_live_match = False

            if not use_live_match:
                # Regular VOD/highlight viewing with diurnal pattern
                # Pick random day within subscription period
                sub_days = (subscription.end_date - max(user.created_at, subscription.start_date)).days
                if sub_days <= 0:
                    continue
                random_day = random_provider.randint(0, sub_days)
                base_date = max(user.created_at, subscription.start_date) + timedelta(days=random_day)

                # Diurnal pattern: prefer 18:00-23:00, avoid 02:00-07:00
                while True:
                    hour = random_provider.randint(0, 23)
                    if 2 <= hour <= 6:
                        if random_provider.random() < 0.9:  # 90% rejection during sleep hours
                            continue
                    break
                
                event_ts = base_date.replace(
                    hour=hour,
                    minute=random_provider.randint(0, 59),
                    second=random_provider.randint(0, 59)
                )

                # Pick a VOD video (highlight or replay)
                vod_videos = [v for v in videos if v["content_type"] in ("highlight", "replay")]
                if not vod_videos:
                    vod_videos = videos
                video = random_provider.choice(vod_videos)

            # Verify temporal validity using TemporalRegistry (Single Source of Truth)
            if not (
                registry.is_valid_event(user.user_id, video["video_id"], event_ts)
                and clock.is_before_horizon(event_ts)
            ):
                continue

            # Temporarily set clock to event time for factory
            clock.advance_to(event_ts)

            event = interaction_factory.create(
                user_id=user.user_id,
                video_id=video["video_id"],
                video_duration=video["duration_sec"],
                event_timestamp=event_ts,
            )
            all_events.append(event)

            remaining_events -= 1
            if remaining_events <= 0:
                break

        if (user_idx + 1) % 1000 == 0:
            print(f"  Generated events for {user_idx + 1}/{len(users)} users ({len(all_events)} total)")

    # Sort events by timestamp for realistic Kafka ordering
    all_events.sort(key=lambda e: e.event_timestamp)

    print(f"  Completed: {len(all_events)} events generated")

    # --------------------------------------------------------
    # Dirty injection for Postgres
    # --------------------------------------------------------

    dirty_injector = PostgresDirtyDataInjector()

    users, subscriptions, payments = dirty_injector.inject(
        users=users,
        subscriptions=subscriptions,
        payments=payments,
    )

    # --------------------------------------------------------
    # 7. Load PostgreSQL
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
    # 8. Kafka - single producer with pre-generated events
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
    # 9. Final report + assertion
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

    # ASSERT at least one event was generated
    assert len(all_events) > 0, f"Expected events to be generated, but got {len(all_events)}"


if __name__ == "__main__":
    main()