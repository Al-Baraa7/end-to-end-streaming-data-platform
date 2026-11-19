"""
MongoDB Silver Layer Validators.

Validation rules for MongoDB-sourced Silver videos table.
Silver validation ONLY asserts data quality - no Gold construction logic.
"""
from pyspark.sql import DataFrame
from scripts.storage.silver.validation.assertions import (
    ValidationSuite,
    assert_pk_unique,
    assert_no_nulls,
    assert_column_values_in_set,
    assert_flag_consistency,
    assert_schema_conformance,
)
from scripts.storage.silver.transforms.mongo import (
    VIDEOS_SILVER_SCHEMA,
    VALID_CONTENT_TYPES,
    VALID_STATUSES,
    VALID_COMPETITIONS,
    DURATION_BOUNDS,
    VALID_STATUS_BY_CONTENT_TYPE,
    KICKOFF_LIVE_TOLERANCE_SEC,
)


def validate_videos_silver(df: DataFrame) -> ValidationSuite:
    """
    Validate Silver MongoDB videos table.

    Checks:
    - PK uniqueness on video_id
    - Required fields not null
    - Data quality flag consistency for all 17 flags
    - Schema conformance
    - Value set validation for content_type, status, competition_id
    """
    suite = ValidationSuite()

    # Primary key uniqueness
    suite.add(assert_pk_unique(df, ["video_id"], "videos_pk_unique"))

    # Required non-null fields (at minimum video_id)
    suite.add(assert_no_nulls(df, ["video_id"], "videos_required_not_null"))

    # Value set validation
    suite.add(assert_column_values_in_set(df, "content_type", VALID_CONTENT_TYPES, "videos_content_type_valid", allow_null=True))
    suite.add(assert_column_values_in_set(df, "status", VALID_STATUSES, "videos_status_valid", allow_null=True))
    suite.add(assert_column_values_in_set(df, "competition_id", VALID_COMPETITIONS, "videos_competition_valid", allow_null=True))

    # ==================== DQ FLAG CONSISTENCY CHECKS (ALL 17 FLAGS) ====================

    # flag_invalid_views_likes
    suite.add(assert_flag_consistency(
        df, "flag_invalid_views_likes", "stats_views",
        "(stats_views IS NOT NULL AND stats_views < 0) OR (stats_likes IS NOT NULL AND stats_likes < 0)", "videos_dq_invalid_views_likes"
    ))

    # flag_missing_live_match
    suite.add(assert_flag_consistency(
        df, "flag_missing_live_match", "content_type",
        "content_type = 'live' AND (match_id IS NULL OR trim(match_id) = '')", "videos_dq_missing_live_match"
    ))

    # flag_invalid_content_type
    suite.add(assert_flag_consistency(
        df, "flag_invalid_content_type", "content_type",
        f"content_type IS NOT NULL AND content_type NOT IN ({','.join(repr(c) for c in VALID_CONTENT_TYPES)})", "videos_dq_invalid_content_type"
    ))

    # flag_invalid_status
    suite.add(assert_flag_consistency(
        df, "flag_invalid_status", "status",
        f"status IS NOT NULL AND status NOT IN ({','.join(repr(s) for s in VALID_STATUSES)})", "videos_dq_invalid_status"
    ))

    # flag_invalid_competition
    suite.add(assert_flag_consistency(
        df, "flag_invalid_competition", "competition_id",
        f"competition_id IS NOT NULL AND competition_id NOT IN ({','.join(repr(c) for c in VALID_COMPETITIONS)})", "videos_dq_invalid_competition"
    ))

    # flag_likes_gt_views
    suite.add(assert_flag_consistency(
        df, "flag_likes_gt_views", "stats_likes",
        "stats_likes IS NOT NULL AND stats_views IS NOT NULL AND stats_likes > stats_views", "videos_dq_likes_gt_views"
    ))

    # flag_invalid_duration (per DURATION_BOUNDS)
    live_bounds = DURATION_BOUNDS["live"]
    highlight_bounds = DURATION_BOUNDS["highlight"]
    replay_bounds = DURATION_BOUNDS["replay"]
    suite.add(assert_flag_consistency(
        df, "flag_invalid_duration", "content_type",
        f"duration_sec IS NOT NULL AND content_type IS NOT NULL AND ("
        f"(content_type = 'live' AND (duration_sec < {live_bounds[0]} OR duration_sec > {live_bounds[1]})) OR "
        f"(content_type = 'highlight' AND (duration_sec < {highlight_bounds[0]} OR duration_sec > {highlight_bounds[1]})) OR "
        f"(content_type = 'replay' AND (duration_sec < {replay_bounds[0]} OR duration_sec > {replay_bounds[1]}))"
        f")", "videos_dq_invalid_duration"
    ))

    # flag_missing_title
    suite.add(assert_flag_consistency(
        df, "flag_missing_title", "title",
        "title IS NULL OR trim(title) = ''", "videos_dq_missing_title"
    ))

    # flag_missing_description
    suite.add(assert_flag_consistency(
        df, "flag_missing_description", "description",
        "description IS NULL OR trim(description) = ''", "videos_dq_missing_description"
    ))

    # flag_missing_live_temporal
    suite.add(assert_flag_consistency(
        df, "flag_missing_live_temporal", "content_type",
        "content_type = 'live' AND (live_start_at IS NULL OR live_end_at IS NULL)", "videos_dq_missing_live_temporal"
    ))

    # flag_invalid_live_temporal_order
    suite.add(assert_flag_consistency(
        df, "flag_invalid_live_temporal_order", "live_start_at",
        "live_start_at IS NOT NULL AND live_end_at IS NOT NULL AND live_start_at >= live_end_at", "videos_dq_invalid_live_temporal_order"
    ))

    # flag_kickoff_live_misalignment
    suite.add(assert_flag_consistency(
        df, "flag_kickoff_live_misalignment", "content_type",
        f"content_type = 'live' AND kickoff_at IS NOT NULL AND live_start_at IS NOT NULL AND abs(unix_timestamp(kickoff_at) - unix_timestamp(live_start_at)) > {KICKOFF_LIVE_TOLERANCE_SEC}", "videos_dq_kickoff_live_misalignment"
    ))

    # flag_created_after_live_start
    suite.add(assert_flag_consistency(
        df, "flag_created_after_live_start", "created_at",
        "created_at IS NOT NULL AND live_start_at IS NOT NULL AND created_at > live_start_at", "videos_dq_created_after_live_start"
    ))

    # flag_last_updated_before_created
    suite.add(assert_flag_consistency(
        df, "flag_last_updated_before_created", "last_updated",
        "last_updated IS NOT NULL AND created_at IS NOT NULL AND last_updated < created_at", "videos_dq_last_updated_before_created"
    ))

    # flag_status_contradiction
    suite.add(assert_flag_consistency(
        df, "flag_status_contradiction", "content_type",
        "content_type IS NOT NULL AND status IS NOT NULL AND NOT ("
        "(content_type = 'live' AND status IN ('streaming', 'ended')) OR "
        "(content_type = 'highlight' AND status = 'archived') OR "
        "(content_type = 'replay' AND status = 'archived')"
        ")", "videos_dq_status_contradiction"
    ))

    # flag_empty_tags
    suite.add(assert_flag_consistency(
        df, "flag_empty_tags", "tags",
        "size(tags) = 0", "videos_dq_empty_tags"
    ))

    # flag_empty_competition_struct
    suite.add(assert_flag_consistency(
        df, "flag_empty_competition_struct", "competition_id",
        "competition_id IS NULL OR trim(competition_id) = ''", "videos_dq_empty_competition_struct"
    ))

    # flag_status_contradiction_live (deterministic: ended but live_start_at > processing timestamp)
    suite.add(assert_flag_consistency(
        df, "flag_status_contradiction_live", "content_type",
        "content_type = 'live' AND status = 'ended' AND live_start_at > silver_processed_at", "videos_dq_status_contradiction_live"
    ))

    # Schema conformance
    suite.add(assert_schema_conformance(df, VIDEOS_SILVER_SCHEMA, "videos_schema", strict=True))

    return suite
