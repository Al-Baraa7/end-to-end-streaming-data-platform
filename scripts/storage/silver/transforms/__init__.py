"""
Silver Layer Transformations Package.

Provides pure, deterministic transformation functions for each source system.
All functions take Bronze DataFrames and return Silver DataFrames with
explicit schema contracts. No I/O, no side effects.
"""
from .postgres import (
    transform_users,
    transform_subscriptions,
    transform_payments,
    transform_all_postgres,
    USERS_SILVER_SCHEMA,
    SUBSCRIPTIONS_SILVER_SCHEMA,
    PAYMENTS_SILVER_SCHEMA,
)

from .kafka import (
    transform_events,
    EVENTS_SILVER_SCHEMA,
    VALID_INTERACTIONS,
    VALID_DEVICES,
    DEVICE_TYPE_MAP,
    SIMULATION_START,
    SIMULATION_END,
    MAX_WATCH_TIME_SEC,
)

from .mongo import (
    transform_videos,
    VIDEOS_SILVER_SCHEMA,
    VALID_CONTENT_TYPES,
    VALID_STATUSES,
    VALID_COMPETITIONS,
    DURATION_BOUNDS,
    WATCH_TIME_BOUNDS,
    VALID_STATUS_BY_CONTENT_TYPE,
    KICKOFF_LIVE_TOLERANCE_SEC,
    LIKE_RATIO_MIN,
    LIKE_RATIO_MAX,
    CONTENT_TYPE_MAP,
)

__all__ = [
    # Postgres
    "transform_users",
    "transform_subscriptions",
    "transform_payments",
    "transform_all_postgres",
    "USERS_SILVER_SCHEMA",
    "SUBSCRIPTIONS_SILVER_SCHEMA",
    "PAYMENTS_SILVER_SCHEMA",
    # Kafka
    "transform_events",
    "EVENTS_SILVER_SCHEMA",
    "VALID_INTERACTIONS",
    "VALID_DEVICES",
    "DEVICE_TYPE_MAP",
    "SIMULATION_START",
    "SIMULATION_END",
    "MAX_WATCH_TIME_SEC",
    # Mongo
    "transform_videos",
    "VIDEOS_SILVER_SCHEMA",
    "VALID_CONTENT_TYPES",
    "VALID_STATUSES",
    "VALID_COMPETITIONS",
    "DURATION_BOUNDS",
    "WATCH_TIME_BOUNDS",
    "VALID_STATUS_BY_CONTENT_TYPE",
    "KICKOFF_LIVE_TOLERANCE_SEC",
    "LIKE_RATIO_MIN",
    "LIKE_RATIO_MAX",
    "CONTENT_TYPE_MAP",
]
