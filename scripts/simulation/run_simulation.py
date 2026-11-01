"""Simulation Orchestrator — Phase 9: End-to-End Pipeline (R1–R13)

Main entry point for the full simulation pipeline:
  1. Phase 1: Temporal isolation (BusinessClock, TemporalRegistry)
  2. Phase 2: Identity & Randomness (IDGenerator, RandomProvider, MatchRegistry)
  3. Phase 3: Kafka Schema & Assembly (InteractionEntity, InteractionFactory)
  4. Phase 4: Session Topology (SessionFactory)
  5. Phase 5: Anomaly Injection (AnomalyCoordinator, *DirtyInjectors)
  6. Phase 6: R6 Payment Rules (BillingEligibility)
  7. Phase 7: R6+R7 Derived Map (BillingEligibility.apply_activation_gate)
  8. Phase 8: Account Status (UserLifecycle)
  9. Phase 9: Invariants (R1/R2, R4, R5, R13)

See README.md#phase-markers-used-in-code-comments
"""

import argparse
from datetime import datetime, timedelta, timezone

from scripts.simulation.clock import BusinessClock
from scripts.simulation.random_provider import RandomProvider
from scripts.simulation.postgres.postgres_simulation import PostgresSimulation
from scripts.simulation.dirty_data.postgres_dirty_injection import (
    PostgresDirtyDataInjector,
)
from scripts.simulation.match_registry import MatchRegistry
from scripts.simulation.mongo.video_simulation import VideoSimulation
from scripts.simulation.dirty_data.mongo_dirty_injection import MongoDirtyDataInjector
from scripts.simulation.dirty_data.kafka_dirty_injection import KafkaDirtyDataInjector
from scripts.simulation.registry.session_factory import (
    SessionFactory,
    compute_like_ratio_target,
    reconcile_likes_with_stats,
)
from scripts.simulation.anomaly_coordinator import GlobalAnomalyCoordinator, create_coordinator

from config import (
    SIM_USER_COUNT,
    SIM_VIDEO_COUNT,
    SIM_KAFKA_EVENT_CEILING,
    SIM_MONGO_BATCH_SIZE,
    SIM_MIN_STATS_VIEWS,
    SIM_MIN_STATS_LIKES,
    SIM_SESSIONS_PER_USER_PER_YEAR,
    SIM_EVENTS_PER_SESSION,
)

from scripts.simulation.loading.load_postgres import load_postgres
from scripts.simulation.loading.load_mongo import load_videos

from scripts.simulation.kafka.video_events_producer import run_producer
from scripts.simulation.temporal_registry import TemporalRegistry

# Canonical R2 / R1 subscription checker (plus the R13 video-reachability checker
# and the Phase 2 R4/R5 match-concept checkers). Defined in
# `scripts.simulation.invariants` (credential-free) so tests and the
# user-lifecycle path can reuse the SAME rules; re-exported here because the
# orchestrator historically imported them from this module.
# These are NOW ACTIVELY CALLED as quality gates in main() - no more noqa blindfolds.
from scripts.simulation.invariants import (
    assert_match_artifact_temporal_integrity,
    assert_match_artifacts_unified,
    assert_subscriptions_non_overlapping,
    assert_video_availability_windows_reachable,
)


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

# ------------------------------------------------------------
# Dynamic volumetric parameters
# ------------------------------------------------------------
# Default volume policy (R13): 1,000 users / 500 videos under a 50,000-event
# CEILING. That stays laptop-friendly while the DERIVED expectation
# (1,000 x 12 sessions x 4 events = 48,000) still exercises the whole
# behaviour model. Everything is resolved from optional env vars (config.py)
# and overridable per run through CLI flags, e.g. a smoke run:
#   python -m scripts.simulation.run_simulation --dry-run --users 50 --videos 50 --events 2400
# The former 10k/10k full-scale dataset is still available explicitly:
#   python -m scripts.simulation.run_simulation --users 10000 --videos 10000 --events 10000

USER_COUNT = SIM_USER_COUNT
VIDEO_COUNT = SIM_VIDEO_COUNT
KAFKA_EVENT_CEILING = SIM_KAFKA_EVENT_CEILING
MONGO_BATCH_SIZE = SIM_MONGO_BATCH_SIZE
MIN_STATS_VIEWS = SIM_MIN_STATS_VIEWS
MIN_STATS_LIKES = SIM_MIN_STATS_LIKES

# ------------------------------------------------------------
# R13 — event-volume model (behaviour FIRST, budget derived)
# ------------------------------------------------------------
# The intended viewing behaviour is declared before any volume is decided:
#   * a user watches roughly SESSIONS_PER_USER_PER_YEAR sessions a year;
#   * each session emits EVENTS_PER_SESSION interactions.
# The event budget is then DERIVED, never fitted:
#   expected_events = USER_COUNT x SESSIONS_PER_USER_PER_YEAR x EVENTS_PER_SESSION
# `--events` remains a CEILING on top of that expectation (D4: the budget is not
# a quota), and per-video Poisson lambdas are bounded by each video's eligible
# audience - they are never rescaled to make the realized count match anything.
SESSIONS_PER_USER_PER_YEAR = SIM_SESSIONS_PER_USER_PER_YEAR
EVENTS_PER_SESSION = SIM_EVENTS_PER_SESSION


def derive_expected_events(
    user_count: int,
    sessions_per_user_per_year: int = SESSIONS_PER_USER_PER_YEAR,
    events_per_session: int = EVENTS_PER_SESSION,
) -> int:
    """
    R13 — derive the expected event volume from viewing behaviour:

        expected_events = users x sessions/user/yr x events/session

    This is the volume the generator EXPECTS to produce. It is deliberately
    independent of `--events`: the ceiling only truncates, it never rescales.
    """
    return (
        int(user_count)
        * int(sessions_per_user_per_year)
        * int(events_per_session)
    )


def build_user_intervals(user_subscriptions_map) -> dict:
    """
    Map each user to the sorted list of their subscription windows `(start, end)`.

    R2 guarantees a user's windows are strictly non-overlapping, so a simple scan
    is a correct membership test (few intervals per user).
    """
    user_intervals = {}
    for user_id, subscriptions in user_subscriptions_map.items():
        if not subscriptions:
            continue
        user_intervals[user_id] = sorted(
            ((s.start_date, s.end_date) for s in subscriptions),
            key=lambda window: window[0],
        )
    return user_intervals


def count_eligible_users(user_intervals, window_start, window_end) -> int:
    """
    Number of users holding a subscription that overlaps [window_start, window_end].

    This is the video's eligible audience: the population that could possibly have
    watched it, and therefore the source of the R13 audience bound.
    """
    eligible = 0
    for intervals in user_intervals.values():
        for start, end in intervals:
            if start > window_end:
                break  # sorted by start: no later window can overlap
            if end >= window_start:
                eligible += 1
                break
    return eligible


def resolve_video_window(video_window):
    """
    Availability window used for event sampling: the live broadcast window for
    live content, otherwise `created_at` up to the simulation horizon.

    Returns `(start, end)` or None when the window is absent/degenerate.
    """
    if (
        video_window.live_start_at is not None
        and video_window.live_end_at is not None
    ):
        start, end = video_window.live_start_at, video_window.live_end_at
    else:
        start, end = video_window.created_at, SIMULATION_END

    if start >= end:
        return None
    return start, end


# `assert_subscriptions_non_overlapping` (R2 non-overlap / R1 horizon) is imported
# above from scripts.simulation.invariants and called by the subscription step below.


def parse_args():
    parser = argparse.ArgumentParser(description="Run data platform simulation")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run simulation without connecting to databases (MongoDB, PostgreSQL, Kafka)"
    )
    parser.add_argument(
        "--users",
        type=int,
        default=USER_COUNT,
        help=f"Number of users to generate (default: {USER_COUNT} from SIM_USER_COUNT)",
    )
    parser.add_argument(
        "--videos",
        type=int,
        default=VIDEO_COUNT,
        help=f"Number of videos to generate (default: {VIDEO_COUNT} from SIM_VIDEO_COUNT)",
    )
    parser.add_argument(
        "--events",
        type=int,
        default=KAFKA_EVENT_CEILING,
        help=(
            "CEILING for generated Kafka events - not a quota (D4). The expected "
            "volume is derived from users x sessions/user/yr x events/session and "
            f"may land below this ceiling (default: {KAFKA_EVENT_CEILING} from "
            "SIM_KAFKA_EVENT_CEILING)"
        ),
    )
    parser.add_argument(
        "--mongo-batch-size",
        type=int,
        default=MONGO_BATCH_SIZE,
        help=f"MongoDB insert batch size (default: {MONGO_BATCH_SIZE} from SIM_MONGO_BATCH_SIZE)",
    )
    parser.add_argument(
        "--min-views",
        type=int,
        default=MIN_STATS_VIEWS,
        help="Floor applied to final video views (0 = exact event count)",
    )
    parser.add_argument(
        "--min-likes",
        type=int,
        default=MIN_STATS_LIKES,
        help="Floor applied to final video likes (0 = exact event count)",
    )
    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

def main():
    args = parse_args()
    DRY_RUN = args.dry_run

    # Dynamic volumetric parameters: CLI flags override the env-driven defaults
    # from config.py. The locals intentionally shadow the module-level defaults
    # so every downstream step uses the resolved (possibly lightweight) values.
    USER_COUNT = args.users
    VIDEO_COUNT = args.videos
    KAFKA_EVENT_CEILING = args.events
    MONGO_BATCH_SIZE = args.mongo_batch_size
    MIN_STATS_VIEWS = args.min_views
    MIN_STATS_LIKES = args.min_likes

    if USER_COUNT <= 0 or VIDEO_COUNT <= 0 or KAFKA_EVENT_CEILING <= 0:
        raise ValueError(
            "users, videos and events must all be > 0; got "
            f"users={USER_COUNT}, videos={VIDEO_COUNT}, events={KAFKA_EVENT_CEILING}"
        )

    # --------------------------------------------------------
    # R13 — the event budget is DERIVED from the viewing-session model:
    #   expected_events = users x sessions/user/yr x events/session
    # `KAFKA_EVENT_CEILING` (--events) stays a CEILING on top of it, and each video's
    # Poisson lambda is that video's share of the derived expectation.
    # --------------------------------------------------------

    expected_events = derive_expected_events(USER_COUNT)
    expected_events_per_video = expected_events / VIDEO_COUNT

    print("=" * 70)
    print("STARTING NEW DATA PLATFORM PIPELINE")
    if DRY_RUN:
        print("MODE: DRY RUN (no database connections)")
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
    print(
        f"Volumes: users={USER_COUNT}, videos={VIDEO_COUNT}, "
        f"mongo batch size={MONGO_BATCH_SIZE}"
    )
    print(
        f"R13 behaviour model: {SESSIONS_PER_USER_PER_YEAR} sessions/user/yr "
        f"x {EVENTS_PER_SESSION} events/session"
    )
    # Target (derived) vs. ceiling (--events): the ceiling only truncates.
    print(
        f"R13 event budget: target(derived)={expected_events}, "
        f"ceiling(--events)={KAFKA_EVENT_CEILING}"
    )

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

    # R4 - ONE registry owns the match catalogue of this run. The generator
    # registers each match exactly once (monotonic `match_000001...` sequence) and
    # stamps every artifact built from it with that same `match_id`, so a match's
    # live broadcast, highlight and replay are joinable instead of being three
    # unrelated videos that each drew their own `randint(1000, 9999)` id.
    match_registry = MatchRegistry()

    video_simulation = VideoSimulation(
        clock=clock,
        random_provider=random_provider,
        simulation_start=START_TIME,
        simulation_end=SIMULATION_END,
        match_registry=match_registry,
        min_stats_views=MIN_STATS_VIEWS,
        min_stats_likes=MIN_STATS_LIKES,
        # R13 — the demand basis is DERIVED from behaviour, never hard-coded:
        # each video's latent popularity (Poisson lambda) is its share of
        # users x sessions/user/yr x events/session. VideoSimulation normalises
        # the declared popularity structure so that
        # sum(latent_popularity) == expected_events across the catalog, which is
        # what removes the need for any post-hoc rescaling pass below.
        expected_events_per_video=expected_events_per_video,
    )

    videos = list(video_simulation.generate_videos(
        count=VIDEO_COUNT
    ))

    # R4/R5 (Phase 2) - gate the CLEAN catalogue, before any dirty-data injection
    # is allowed to take it apart (injected defects are the point of the dirty
    # layer; the generator itself must never produce them):
    #   R4: the live, highlight and replay artifacts of a match share ONE
    #       registered `match_id` and one set of match metadata;
    #   R5: created_at <= live_start_at, kickoff_at + duration_sec <= horizon and
    #       duration_sec consistent with the match window.
    assert_match_artifacts_unified(videos)
    assert_match_artifact_temporal_integrity(
        videos,
        simulation_end=SIMULATION_END,
    )

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
    
    video_ids = [v["video_id"] for v in videos]

    print("\n[3] Video generation completed (MongoDB persistence deferred to [6b])")
    print(f"  Generated {len(video_ids)} videos ({len(live_matches)} live matches)")
    print(
        f"  R4 match registry: {len(match_registry)} match(es) for "
        f"{len(video_ids)} artifact(s) - R4/R5 invariants passed "
        "(one match_id per match, shared by live/highlight/replay)"
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
    truncated_subscriptions = 0

    for user_idx, user in enumerate(users):
        # R1/R2/R8 — ONE domain call per user builds the whole journey, in the
        # canonical order: sequential subscriptions (R2) -> payment chains
        # (R1, on child clocks forked at each window's start so the master cursor
        # never moves) -> account_status evaluated at SIMULATION_END (R8) ->
        # last_login (R8).
        # The rules live in scripts/simulation/postgres/*; the orchestrator only
        # collects the result and never re-implements the ordering.
        journey = postgres_simulation.generate_user_journey(
            user=user,
            subscription_count=random_provider.randint(
                postgres_simulation.MIN_SUBSCRIPTIONS_PER_USER,
                postgres_simulation.MAX_SUBSCRIPTIONS_PER_USER,
            ),
        )

        subscriptions.extend(journey.subscriptions)
        payments.extend(journey.payments)

        # R1 telemetry: windows clamped at SIMULATION_END.
        truncated_subscriptions += journey.truncated_subscription_count

        user_subscriptions_map[user.user_id] = journey.subscriptions

        if (user_idx + 1) % 1000 == 0:
            print(f"  Processed {user_idx + 1}/{len(users)} users for subscriptions")

    # NOTE (R1): the master clock intentionally stays at the end of the step-[4]
    # user-creation timeline. It is no longer rewound to users[-1].created_at —
    # every per-user temporal operation happens on forked child clocks.
    print(f"  Completed: {len(subscriptions)} subscriptions, {len(payments)} payments")
    if truncated_subscriptions:
        print(
            f"  Horizon clamp: {truncated_subscriptions} subscription(s) truncated "
            f"at SIMULATION_END"
        )

    # POST-GENERATION ASSERTIONS (R2 non-overlap + R1 horizon containment)
    assert_subscriptions_non_overlapping(
        user_subscriptions_map,
        simulation_end=SIMULATION_END,
    )
    print("  Assertion passed: per-user subscriptions are strictly non-overlapping")

    # --------------------------------------------------------
    # R6 + R7 — PAYMENTS ARE THE ACTIVATION GATE
    # --------------------------------------------------------
    # R6: re-resolve every subscription's persisted `is_active` from its OWN payment
    #     chain (has_success_payment AND start <= SIMULATION_END <= end, free plans
    #     exempt) instead of trusting the temporal-only value the factory could
    #     compute before any payment existed.
    # R7: derive the eligibility map the event stream is allowed to use — windows
    #     that were ACTUALLY PAID FOR and accounts that are STILL ACTIVE.
    # The rules live in scripts/simulation/registry/billing_eligibility.py, so this
    # orchestrator only resolves the map once and hands it to step [6].
    billing_eligibility = postgres_simulation.resolve_billing_eligibility(
        users=users,
        subscriptions=subscriptions,
        payments=payments,
    )

    coverage = billing_eligibility.coverage_report()
    print(
        f"  R6 activation gate: {coverage.subscriptions_active}/"
        f"{coverage.subscriptions_total} subscription(s) in force and paid "
        f"({coverage.subscriptions_unpaid} unpaid premium window(s), "
        f"{billing_eligibility.deactivated_by_payment_gate} deactivated by the "
        "payment gate)",
        flush=True,
    )
    print(
        f"  R7/R8 eligibility: {coverage.eligible_users}/{coverage.users_total} "
        f"user(s) active with a paid window, "
        f"{coverage.eligible_windows} eligible window(s) "
        f"(accounts: active={coverage.users_active}, "
        f"suspended={coverage.users_suspended}, deleted={coverage.users_deleted})",
        flush=True,
    )

    # --------------------------------------------------------
    # PHASE 3: Generate Kafka events using SESSION-CENTRIC model (R3)
    # - Sessions cluster around live kickoff ± window and VOD prime-time (R10)
    # - Within-session dwell-time distributions drive event sequences
    # - Like ratios derived from LIKE_RATIO_MIN/MAX, then reconciled (R11)
    # - Stats derived from actual generated events (perfect coupling, R13)
    # --------------------------------------------------------

    print("\n[6] Generating Kafka watch events with session-centric model...")

    # R1 temporal isolation: event generation owns a FORKED cursor.
    # Every event used to be applied to the master clock, and since video
    # timestamps are sampled in random order that cursor jumped backwards
    # repeatedly, so "Final BusinessClock" only ever reported whichever
    # timestamp happened to come last (drift D2 / backlog R1b). The forked
    # cursor keeps the master clock a stable, meaningful report value while
    # preserving the exact semantics the factory expects.
    event_clock = clock.fork()

    # Initialize TemporalRegistry as Single Source of Truth for referential integrity.
    # R7 — the registry only LOADS windows that were actually PAID FOR and only
    # serves users whose account is STILL ACTIVE, so step [6] structurally cannot
    # place an event on a free-riding premium window or on a suspended/deleted
    # account. Gating happens here, at load time, so the inner probe loop stays a
    # plain interval scan.
    registry = TemporalRegistry()
    registry.load_from_memory(
        [s.to_dict() for s in subscriptions],
        videos,
        paid_subscription_ids=billing_eligibility.paid_subscription_ids(),
        active_user_ids=billing_eligibility.active_user_ids(),
    )
    print(
        f"  Registry eligibility gate: {registry.gated_out_subscription_count} "
        f"unpaid window(s) excluded from the registry",
        flush=True,
    )

    # R13 — canonical invariant (scripts.simulation.invariants): every video must own
    # at least one timestamp the registry can serve (`created_at <= live_end_at` for
    # live content). A live video whose broadcast window ends before it was created
    # can never place an event: all 20 probes of every draw fail and 100% of its
    # Poisson draws are dropped. That was the real state of 78 of 151 live videos
    # (~31% of the modelled volume), invisible while the rescaling pass inflated
    # other lambdas to compensate. Fail fast instead of quietly losing the volume.
    assert_video_availability_windows_reachable(registry.video_windows.values())
    print("  Assertion passed: every video has a servable availability window")
    # Phase 4 - Session Topology: Initialize SessionFactory with registry and clock
    from scripts.simulation.registry.session_factory import SessionFactory
    from scripts.simulation.mongo.video_simulation import VideoSimulation as VideoSimulationClass
    
    LIKE_RATIO_MIN = VideoSimulationClass.LIKE_RATIO_MIN
    LIKE_RATIO_MAX = VideoSimulationClass.LIKE_RATIO_MAX
    
    # First, assign engagement tiers to all users (class method)
    user_ids = [u.user_id for u in users]
    user_profiles = SessionFactory.assign_user_profiles(user_ids, random_provider)
    print(f"  Assigned engagement tiers to {len(user_ids)} users: "
          f"HIGH={sum(1 for p in user_profiles.values() if p.engagement_tier.value == 'high')}, "
          f"MEDIUM={sum(1 for p in user_profiles.values() if p.engagement_tier.value == 'medium')}, "
          f"LOW={sum(1 for p in user_profiles.values() if p.engagement_tier.value == 'low')}")
    
    # Now create SessionFactory instance with videos and user_profiles
    session_factory = SessionFactory(
        clock=event_clock,
        random_provider=random_provider,
        registry=registry,
        videos=videos,
        user_profiles=user_profiles,
        like_ratio_min=LIKE_RATIO_MIN,
        like_ratio_max=LIKE_RATIO_MAX,
    )

    # Phase 4 - Session Topology: Generate sessions for all users
    all_sessions = []
    total_sessions_planned = 0
    
    # Calculate simulation duration in years for session count scaling
    sim_duration_years = (SIMULATION_END - clock.now()).total_seconds() / (365.25 * 24 * 3600)
    
    for user_id in user_ids:
        profile = user_profiles[user_id]
        # Scale sessions_per_year by simulation duration (M-3: round to eliminate systematic truncation deficit)
        target_sessions = max(1, round(profile.sessions_per_year * sim_duration_years))
        user_sessions = session_factory.generate_user_sessions(
            user_id=user_id,
            simulation_start=clock.now(),
            simulation_end=SIMULATION_END,
            target_session_count=target_sessions,
        )
        all_sessions.extend(user_sessions)
        total_sessions_planned += len(user_sessions)

    print(f"  Generated {len(all_sessions)} sessions across {len(user_ids)} users (planned: {total_sessions_planned})")

    # Phase 4 - Session Topology: Flatten sessions into individual Kafka events
    all_events = []
    event_ceiling = KAFKA_EVENT_CEILING  # --events is a CEILING (D4)
    
    for session in all_sessions:
        if len(all_events) >= event_ceiling:
            break
        # Each session already contains properly ordered events with timestamps
        for event in session.events:
            if len(all_events) >= event_ceiling:
                break
            all_events.append(event)
    
    # Sort events by timestamp for realistic Kafka ordering
    all_events.sort(key=lambda e: e.event_timestamp)

    # M-2 REMEDIATION: Sync sessions to truncated event stream
    # When ceiling binds, session aggregates (play_count, like_count) must reflect
    # only the events that actually survive truncation, otherwise stats↔events drift.
    if len(all_events) < sum(len(s.events) for s in all_sessions):
        # Ceiling was hit - rebuild sessions from surviving events only
        surviving_events = set(all_events)
        synced_sessions = []
        for session in all_sessions:
            # Filter to only events that survived truncation
            filtered_events = [e for e in session.events if e in surviving_events]
            if not filtered_events:
                continue  # Session fully truncated away
            # Create new session with filtered events (play_count/like_count are computed properties)
            from scripts.simulation.registry.session_factory import Session
            synced_session = Session(
                session_id=session.session_id,
                user_id=session.user_id,
                video_id=session.video_id,
                start_ts=filtered_events[0].event_timestamp,
                end_ts=filtered_events[-1].event_timestamp,
                content_type=session.content_type,
                device_type=session.device_type,
                engagement_tier=session.engagement_tier,
                events=filtered_events,
            )
            synced_sessions.append(synced_session)
        all_sessions = synced_sessions

    # Phase 4 - Like Reconciliation (R11): Adjust like count to target ratio (0.5%-3%)
    # Using LIKE_RATIO_MIN/MAX from SessionFactory constants
    all_sessions, videos = reconcile_likes_with_stats(
        all_sessions, videos, LIKE_RATIO_MIN, LIKE_RATIO_MAX
    )

    realized_events = len(all_events)

    print(
        f"  Completed: {realized_events} events generated from {len(all_sessions)} sessions "
        f"across {len(videos)} videos"
    )
    
    # R13 � TARGET vs. REALIZED accounting for session-centric model
    total_plays = sum(s.play_count for s in all_sessions)
    total_likes = sum(s.like_count for s in all_sessions)
    target_likes = compute_like_ratio_target(total_plays, LIKE_RATIO_MIN, LIKE_RATIO_MAX)
    
    print(f"  Session metrics: total_plays={total_plays}, total_likes={total_likes}")
    print(f"  Like ratio: actual={total_likes/total_plays*100:.2f}% " 
          f"(target={target_likes/total_plays*100:.2f}%, range={LIKE_RATIO_MIN*100:.1f}%-{LIKE_RATIO_MAX*100:.1f}%)")
    print(f"  Target vs. realized: derived target={expected_events}, "
          f"realized={realized_events}, ceiling={event_ceiling}")
    if realized_events < expected_events:
        print(
            f"  Under derived target by {expected_events - realized_events} "
            "event(s) - temporal filtering and eligibility gating, NOT rescaled"
        )
    elif realized_events > expected_events:
        print(
            f"  Over derived target by {realized_events - expected_events} "
            "event(s) - session variability around derived mean, NOT rescaled"
        )
    if realized_events >= event_ceiling:
        print(
            f"  NOTE: ceiling reached at {event_ceiling} - the realized stream "
            "was truncated, not rescaled"
        )
    # Validation: Check stats↔events coupling
    total_play_events = sum(1 for e in all_events if e.interaction_type == "play")
    total_like_events = sum(1 for e in all_events if e.interaction_type == "like")
    total_video_views = sum(v["stats"]["views"] for v in videos)
    total_video_likes = sum(v["stats"]["likes"] for v in videos)
    
    print(f"  Validation: play_events={total_play_events}, video_stats.views={total_video_views}")
    print(f"  Validation: like_events={total_like_events}, video_stats.likes={total_video_likes}")
    print(f"  Coupling ratio (views/plays): {total_video_views/total_play_events:.2f}" if total_play_events > 0 else "  No play events")

    # --------------------------------------------------------
    # LIVE INVARIANT QUALITY GATE (pre-dirty-injection)
    # Validate clean memory baseline is pristine before any corruption
    # --------------------------------------------------------
    
    print("\n[Invariant Gate] Running pre-injection quality checks...")
    
    # R1/R2: Subscription non-overlapping check (use user_subscriptions_map which is a dict)
    assert_subscriptions_non_overlapping(user_subscriptions_map, simulation_end=SIMULATION_END)
    print("  [OK] assert_subscriptions_non_overlapping passed")
    
    # R4/R5: Match artifact temporal integrity
    assert_match_artifact_temporal_integrity(videos, simulation_end=SIMULATION_END)
    print("  [OK] assert_match_artifact_temporal_integrity passed")
    
    # R4: Match artifacts unified
    assert_match_artifacts_unified(videos)
    print("  [OK] assert_match_artifacts_unified passed")
    
    # R13: Video availability windows reachable
    assert_video_availability_windows_reachable(registry.video_windows.values())
    print("  [OK] assert_video_availability_windows_reachable passed")
    
    print("[Invariant Gate] All pre-injection checks PASSED - clean baseline confirmed")

    # --------------------------------------------------------
    # GLOBAL ANOMALY COORDINATOR SETUP
    # Create with actual volumes for volume-weighted blended 3.5% rate
    subscription_count = len(subscriptions)
    payment_count = len(payments)
    video_count = len(videos)
    event_count = len(all_events)
    
    coordinator = create_coordinator(
        seed=RANDOM_SEED,
        user_count=USER_COUNT,
        subscription_count=subscription_count,
        payment_count=payment_count,
        video_count=video_count,
        event_count=event_count,
    )
    
    print(f"  GlobalAnomalyCoordinator initialized: blended_rate={coordinator.blended_rate:.4f} (target 0.035)")
    print(f"  Volumes: users={USER_COUNT}, subscriptions={subscription_count}, payments={payment_count}, videos={video_count}, events={event_count}")
    
    # Get per-stream rates from coordinator
    pg_rates = coordinator.get_injector_kwargs()["postgres"]
    mongo_rates = coordinator.get_injector_kwargs()["mongo"]
    kafka_rates = coordinator.get_injector_kwargs()["kafka"]
    
    # Create injectors with SHARED RandomProvider from coordinator
    dirty_injector = PostgresDirtyDataInjector(random_provider=coordinator.random_provider)
    mongo_dirty_injector = MongoDirtyDataInjector(random_provider=coordinator.random_provider)
    kafka_dirty_injector = KafkaDirtyDataInjector(random_provider=coordinator.random_provider)

    # --------------------------------------------------------
    # Dirty injection for Postgres (coordinator-enforced rates)
    # --------------------------------------------------------

    print("  Applying dirty data injection (deepcopy)...", flush=True)

    # Convert iterators to lists to maintain compatibility with downstream code
    users = list(dirty_injector.inject_users(users, pg_rates["user_anomaly_rate"]))
    subscriptions = list(dirty_injector.inject_subscriptions(subscriptions, pg_rates["subscription_anomaly_rate"]))
    payments = list(dirty_injector.inject_payments(payments, pg_rates["payment_anomaly_rate"]))
    
    # MongoDB dirty injection (coordinator-enforced rate)
    videos = list(mongo_dirty_injector.inject(videos=videos, anomaly_rate=mongo_rates["anomaly_rate"]))
    print("  MongoDB dirty injection completed", flush=True)
    
    print("  Dirty injection completed", flush=True)

    # --------------------------------------------------------
    # POST-INJECTION VALIDATION GATE (D5)
    # Verify anomaly integrity and well-formedness before persistence
    # --------------------------------------------------------
    
    print("\n[Post-Injection Gate] Verifying anomaly integrity...")

    # 1. Critical IDs preserved on all mutated entities
    assert all(hasattr(u, "user_id") and u.user_id for u in users), "User user_id missing or null"
    assert all(hasattr(s, "subscription_id") and s.subscription_id for s in subscriptions), "Subscription subscription_id missing or null"
    assert all(hasattr(p, "payment_id") and p.payment_id for p in payments), "Payment payment_id missing or null"
    assert all(v.get("video_id") for v in videos), "Video video_id missing or null"

    # 2. Anomaly count telemetry for MongoDB videos
    def count_anomalous_videos(vids):
        """Count videos carrying at least one injected anomaly."""
        count = 0
        for v in vids:
            stats = v.get("stats", {})
            if (
                v.get("title") is None
                or not isinstance(v.get("duration_sec"), int) or v.get("duration_sec", 1) <= 0
                or v.get("content_type") not in ("live", "highlight", "replay")
                or (v.get("content_type") == "live" and v.get("match_id") is None)
                or not isinstance(stats.get("views"), int) or not isinstance(stats.get("likes"), int)
                or stats.get("likes", 0) > stats.get("views", 0)
                or not v.get("competition")
            ):
                count += 1
        return count

    actual_mongo_anomalies = count_anomalous_videos(videos)
    expected_mongo_anomalies = int(len(videos) * mongo_rates["anomaly_rate"])
    print(f"  Mongo anomalies: expected~{expected_mongo_anomalies}, actual={actual_mongo_anomalies}")

    # 3. Anomaly well-formedness: type safety on critical numeric fields
    for v in videos:
        # duration_sec must remain int (even if semantically invalid)
        assert isinstance(v.get("duration_sec"), int), f"duration_sec corrupted to non-int: {v.get('duration_sec')!r}"
        # stats fields must remain int
        stats = v.get("stats", {})
        assert isinstance(stats.get("views"), int), f"stats.views corrupted to non-int: {stats.get('views')!r}"
        assert isinstance(stats.get("likes"), int), f"stats.likes corrupted to non-int: {stats.get('likes')!r}"
        # video_id must remain str
        assert isinstance(v.get("video_id"), str), f"video_id corrupted to non-str: {v.get('video_id')!r}"
        # content_type must remain str
        assert isinstance(v.get("content_type"), str), f"content_type corrupted to non-str: {v.get('content_type')!r}"

    # 4. PostgreSQL entity type safety spot-checks
    for u in users:
        assert isinstance(u.user_id, str), f"User user_id corrupted: {u.user_id!r}"
    for s in subscriptions:
        assert isinstance(s.subscription_id, str), f"Subscription subscription_id corrupted: {s.subscription_id!r}"
        assert isinstance(s.user_id, str), f"Subscription user_id corrupted: {s.user_id!r}"
    for p in payments:
        assert isinstance(p.payment_id, str), f"Payment payment_id corrupted: {p.payment_id!r}"
        assert isinstance(p.subscription_id, str), f"Payment subscription_id corrupted: {p.subscription_id!r}"

    print("[Post-Injection Gate] All anomaly integrity checks PASSED")

    # --------------------------------------------------------
    # 6b. Persist videos AFTER stats <-> events coupling AND dirty injection
    # The documents written to MongoDB now carry the stats that are actually
    # derived from the generated event stream, AND the injected anomalies.
    # --------------------------------------------------------

    if not DRY_RUN:
        print("\n[6b] Loading MongoDB (post-coupling stats + anomalies)...")

        video_ids = load_videos(
            videos=videos,
            batch_size=MONGO_BATCH_SIZE,
        )

        print(
            f"[6b] MongoDB loading completed: "
            f"{len(video_ids)} videos ({len(live_matches)} live matches)"
        )
    else:
        print("\n[6b] Skipping MongoDB loading (dry-run)")
        print(f"  Would load {len(videos)} videos ({len(live_matches)} live matches)")

    # --------------------------------------------------------
    # Guard: payments must be flat PaymentEntity objects.
    # A nested list (or None) here would crash the Postgres loader with an
    # AttributeError, so fail loudly and early (also during dry-run).
    # --------------------------------------------------------

    assert all(hasattr(p, "payment_id") for p in payments), (
        "payments contains non-PaymentEntity entries: "
        f"{[type(p).__name__ for p in payments[:5]]}"
    )

    # --------------------------------------------------------
    # 7. Load PostgreSQL (users -> subscriptions -> payments)
    # --------------------------------------------------------

    if not DRY_RUN:
        print("\n[7] Loading PostgreSQL...")

        load_postgres(
            users=users,
            subscriptions=subscriptions,
            payments=payments,
        )

        print(
            f"[7] PostgreSQL loading completed: {len(users)} users, "
            f"{len(subscriptions)} subscriptions, {len(payments)} payments"
        )
    else:
        print("\n[7] Skipping PostgreSQL loading (dry-run)")
        print(
            f"  Would load {len(users)} users, {len(subscriptions)} subscriptions, "
            f"{len(payments)} payments"
        )

    if not DRY_RUN:
        print("\n[8] Starting Kafka producer...")

        # Kafka dirty injection using coordinator rate and shared RandomProvider
        all_events = list(kafka_dirty_injector.inject(events=all_events, anomaly_rate=kafka_rates["anomaly_rate"]))

        # R13: `event_ceiling` is passed as `event_count` only for interface
        # compatibility. Because pre-generated `events` are supplied, the
        # producer merely drains the stream (len(all_events) <= ceiling), so the
        # ceiling can never be converted into a quota here.
        run_producer(
            clock=clock,
            random_provider=random_provider,
            user_ids=[u.user_id for u in users],
            video_ids=video_ids,
            event_count=event_ceiling,
            dirty_injector=kafka_dirty_injector,
            anomaly_rate=0.0,  # Events already injected at line 816; avoid double injection
            events=all_events,  # NEW: pass pre-generated events
        )

        print(
            "[8] Kafka producer completed"
        )
    else:
        print("\n[8] Skipping Kafka producer (dry-run)")
        print(f"  Would send {len(all_events)} events")

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

    # R13 accounting: derived target vs. realized, with `--events` shown as the
    # ceiling it actually is (not a quota).
    print(
        "Kafka events target(derived):",
        expected_events,
    )

    print(
        "Kafka events ceiling(--events):",
        event_ceiling,
    )

    print(
        "Final BusinessClock (master cursor):",
        clock.now(),
    )

    if all_events:
        print(
            "Last event timestamp:",
            max(event.event_timestamp for event in all_events),
        )

    print("=" * 70)

    # ASSERT at least one event was generated
    assert len(all_events) > 0, f"Expected events to be generated, but got {len(all_events)}"

    # R13 invariant: `--events` is a CEILING, so the realized stream may never
    # exceed it. Landing UNDER the derived target is legitimate (Poisson noise
    # plus audience bounds) and is reported above, never rescaled to fit.
    assert realized_events <= event_ceiling, (
        f"Ceiling violated: realized {realized_events} > ceiling {event_ceiling}"
    )


if __name__ == "__main__":
    main()