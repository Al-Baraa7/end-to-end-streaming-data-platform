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
)

from .mongo import (
    transform_videos,
    transform_videos_clean,
    VIDEOS_SILVER_SCHEMA,
    VALID_CONTENT_TYPES,
    VALID_STATUSES,
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
    # Mongo
    "transform_videos",
    "transform_videos_clean",
    "VIDEOS_SILVER_SCHEMA",
    "VALID_CONTENT_TYPES",
    "VALID_STATUSES",
]