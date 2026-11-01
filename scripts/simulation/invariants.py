"""Invariants — Phase 9: Invariants & Validation

Canonical invariant checkers (R1, R2, R4, R5, R13). No credential-dependent logic — reused by tests and pipeline.
See README.md#phase-markers-used-in-code-comments
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Union
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SubscriptionInterval:
    """Immutable subscription validity window with .start/.end attributes."""
    user_id: str
    start: datetime
    end: datetime


class SubscriptionEntity:
    """Postgres subscription entity with .start_date/.end_date attributes."""
    def __init__(
        self,
        *,
        subscription_id: str,
        user_id: str,
        plan_type: str,
        start_date: datetime,
        end_date: datetime | None,
        is_active: bool,
        auto_renew: bool,
        nominal_duration_days: int | None = None,
    ):
        self.subscription_id = subscription_id
        self.user_id = user_id
        self.plan_type = plan_type
        self.start_date = start_date
        self.end_date = end_date
        self.is_active = is_active
        self.auto_renew = auto_renew
        self.nominal_duration_days = nominal_duration_days


def _normalize_subscription_interval(interval: Union[SubscriptionInterval, SubscriptionEntity]) -> SubscriptionInterval:
    """Convert a SubscriptionEntity or SubscriptionInterval to a normalized SubscriptionInterval."""
    if isinstance(interval, SubscriptionInterval):
        return interval
    # Assume SubscriptionEntity
    return SubscriptionInterval(
        user_id=interval.user_id,
        start=interval.start_date,
        end=interval.end_date if interval.end_date is not None else interval.start_date,
    )


# -- Phase 9: R1/R2 Subscription Non-Overlapping & Horizon Containment --

def assert_subscriptions_non_overlapping(
    user_subscriptions_map: Dict[str, List[Union[SubscriptionInterval, SubscriptionEntity]]],
    *,
    simulation_end: datetime,
) -> None:
    """
    Validate that all per-user subscriptions are:
    1. Strictly non-overlapping (start_{i+1} > end_i)
    2. Within the simulation horizon (end <= simulation_end)
    3. Sequential with gaps between subscriptions

    This is the canonical invariant checker used across the pipeline:
    - Tests and user-lifecycle path reuse this SAME rule
    - No credential-dependent logic
    Accepts either SubscriptionInterval (with .start/.end) or SubscriptionEntity (with .start_date/.end_date).
    """
    for user_id, intervals in user_subscriptions_map.items():
        if not intervals:
            continue

        # Normalize all intervals to SubscriptionInterval for consistent attribute access
        normalized = [_normalize_subscription_interval(i) for i in intervals]

        # Sort by start date for sequential validation
        sorted_intervals = sorted(normalized, key=lambda i: i.start)

        for i, interval in enumerate(sorted_intervals):
            # Rule R2: Non-overlapping - each subscription must start after the previous ends
            if i > 0:
                prev = sorted_intervals[i - 1]
                # Strictly non-overlapping: next start must be > previous end
                if interval.start <= prev.end:
                    raise ValueError(
                        f"User {user_id}: Subscriptions overlap! "
                        f"Sub {i-1} ends at {prev.end}, "
                        f"Sub {i} starts at {interval.start}"
                    )

            # Rule R1: Horizon containment - must not exceed simulation end
            if interval.end > simulation_end:
                raise ValueError(
                    f"User {user_id}: Subscription exceeds simulation horizon: "
                    f"{interval.end} > {simulation_end}"
                )


class _VideoAvailabilityWindow(Protocol):
    """
    Anything exposing the `TemporalRegistry` video-window attributes.

    Declared as a Protocol so this module stays standalone (no import of the
    registry) while still typing the checker precisely.
    """
    video_id: str
    created_at: datetime
    live_start_at: Optional[datetime]
    live_end_at: Optional[datetime]


# -- Phase 9: R13 Video Reachability (Popularity Normalizer) --

def assert_video_availability_windows_reachable(
    video_windows: Iterable[_VideoAvailabilityWindow],
) -> None:
    """
    Validate that every video can serve at least one timestamp (R13).

    The rule mirrors `TemporalRegistry.video_is_available`:
    - VOD (highlight/replay): available from `created_at` onwards -> always reachable
    - Live: available only inside `[live_start_at, live_end_at]` AND at/after
      `created_at`, so reachability requires `live_end_at > created_at`

    A live video whose broadcast window ends before the video was created can never
    receive an event: the registry rejects every candidate timestamp, so the emitter
    burns its whole probe budget and silently drops 100% of that video's Poisson
    draws. This was real (78 of 151 live videos, ~31% of the modelled volume) and was
    previously invisible because the removed rescaling pass inflated other lambdas to
    compensate. Fail fast instead of losing a share of the volume.
    """
    unreachable: List[str] = []

    for window in video_windows:
        # VOD content has no broadcast window: creation is the only gate.
        if window.live_start_at is None or window.live_end_at is None:
            continue
        if window.live_end_at <= window.created_at:
            unreachable.append(window.video_id)

    if unreachable:
        preview = ", ".join(unreachable[:5])
        suffix = f" (+{len(unreachable) - 5} more)" if len(unreachable) > 5 else ""
        raise ValueError(
            f"{len(unreachable)} video(s) have an availability window that ends "
            f"before created_at and can therefore never serve an event: "
            f"{preview}{suffix}"
        )


# -- Phase 9: R4 Match Concept (Single Source of Truth) --

def assert_match_artifacts_unified(videos: Iterable[Mapping[str, Any]]) -> None:
    """
    Validate the R4 match concept: one match, one set of metadata, many artifacts.

    A match is registered ONCE by `MatchRegistry` (a `match_id` from a monotonic
    sequence) together with its two teams, its competition and its kick-off
    instant. Its live broadcast, its highlight reel and its full replay are three
    artifacts OF that match, so all of them must carry the same match metadata.
    The previous generator drew a fresh `randint(1000, 9999)` per video, which
    both collided across matches and split single matches into unrelated ids.

    Rules enforced here:
    * every artifact carries a `match_id` (an unregistered artifact cannot be
      joined to a match, so it is unusable downstream);
    * `kickoff_at`, `match_duration_sec` and the whole `competition` sub-document
      (id, name, teams) are IDENTICAL across the artifacts of one match;
    * a match never carries two artifacts of the same content type (that would be
      a duplicate live broadcast / highlight / replay for one match);
    * a match never carries more artifacts than there are content types.
    """
    by_match: Dict[str, List[Mapping[str, Any]]] = {}
    unregistered: List[str] = []

    for video in videos:
        match_id = video.get("match_id")
        if not match_id:
            unregistered.append(str(video.get("video_id", "<unknown>")))
            continue
        by_match.setdefault(str(match_id), []).append(video)

    if unregistered:
        preview = ", ".join(unregistered[:5])
        suffix = f" (+{len(unregistered) - 5} more)" if len(unregistered) > 5 else ""
        raise ValueError(
            f"{len(unregistered)} video(s) have no match_id and can therefore "
            f"not be joined to a match: {preview}{suffix}"
        )

    shared_fields = ("kickoff_at", "match_duration_sec", "competition")
    content_types = ("live", "highlight", "replay")
    problems: List[str] = []

    for match_id, artifacts in by_match.items():
        seen_types: Dict[Any, Any] = {}
        for artifact in artifacts:
            content_type = artifact.get("content_type")
            if content_type in seen_types:
                problems.append(
                    f"{match_id}: duplicate {content_type!r} artifacts "
                    f"({seen_types[content_type]} and {artifact.get('video_id')})"
                )
            else:
                seen_types[content_type] = artifact.get("video_id")

        if len(artifacts) > len(content_types):
            problems.append(
                f"{match_id}: {len(artifacts)} artifacts for a match that has "
                f"only {len(content_types)} content types "
                f"{content_types}"
            )

        reference = artifacts[0]
        for field_name in shared_fields:
            expected = reference.get(field_name)
            for artifact in artifacts[1:]:
                if artifact.get(field_name) != expected:
                    problems.append(
                        f"{match_id}: {field_name} differs between artifacts "
                        f"{reference.get('video_id')} and "
                        f"{artifact.get('video_id')} "
                        f"({expected!r} != {artifact.get(field_name)!r})"
                    )

    if problems:
        preview = "; ".join(problems[:5])
        suffix = f" (+{len(problems) - 5} more)" if len(problems) > 5 else ""
        raise ValueError(
            f"{len(problems)} match metadata inconsistency(ies) found - the live, "
            f"highlight and replay artifacts of a match must share the exact same "
            f"match_id, teams, competition and kick-off: {preview}{suffix}"
        )


# -- Phase 9: R5 Video Temporal Integrity --

def assert_match_artifact_temporal_integrity(
    videos: Iterable[Mapping[str, Any]],
    *,
    simulation_end: datetime,
) -> None:
    """
    Validate that every video artifact's temporal window is self-consistent and
    inside the simulation horizon (R5).

    The match window of an artifact is `[kickoff_at, kickoff_at +
    match_duration_sec]` (both shared with the artifact's siblings, see
    `assert_match_artifacts_unified`). Rules:

    Every artifact
    * `created_at` (and `last_updated`) are timezone-aware and `<= simulation_end`;
    * `duration_sec` is a positive int bounded by the match window - a clip can
      never outrun the match it was cut from;
    * content type and availability window agree: `live` artifacts have a
      broadcast window, highlight/replay (VOD) artifacts do not.

    Live artifacts (broadcast window == the match window)
    * `live_start_at == kickoff_at`, `live_end_at == kickoff_at + match_duration_sec`;
    * `created_at <= live_start_at` - a broadcast is announced at or before
      kick-off, never after it;
    * `live_start_at + duration_sec <= simulation_end` - the whole broadcast fits
      inside the horizon instead of being truncated by it;
    * `duration_sec == live_end_at - live_start_at` - playtime and broadcast
      window are the same number, so they cannot drift apart.

    VOD artifacts
    * `created_at >= kickoff_at + match_duration_sec` - a highlight/replay only
      exists once the final whistle has gone (they are published from the match
      window's end, not from an arbitrary timestamp).
    """
    problems: List[str] = []

    for video in videos:
        video_id = str(video.get("video_id", "<unknown>"))
        content_type = video.get("content_type")
        created_at = video.get("created_at")
        last_updated = video.get("last_updated")
        kickoff_at = video.get("kickoff_at")
        match_duration_sec = video.get("match_duration_sec")
        duration_sec = video.get("duration_sec")
        live_start_at = video.get("live_start_at")
        live_end_at = video.get("live_end_at")

        def fail(message: str) -> None:
            problems.append(f"{video_id}: {message}")

        if content_type not in ("live", "highlight", "replay"):
            fail(
                f"unknown content_type {content_type!r}; expected one of "
                f"('live', 'highlight', 'replay')"
            )
            continue
        if not isinstance(created_at, datetime) or created_at.tzinfo is None:
            fail(f"created_at must be a timezone-aware datetime, got {created_at!r}")
            continue
        if not isinstance(kickoff_at, datetime) or kickoff_at.tzinfo is None:
            fail(f"kickoff_at must be a timezone-aware datetime, got {kickoff_at!r}")
            continue
        if not isinstance(match_duration_sec, int) or match_duration_sec <= 0:
            fail(
                f"match_duration_sec must be a positive int, got "
                f"{match_duration_sec!r}"
            )
            continue

        match_end_at = kickoff_at + timedelta(seconds=match_duration_sec)

        if created_at > simulation_end:
            fail(
                f"created_at {created_at} is after the simulation horizon "
                f"{simulation_end}"
            )
        if isinstance(last_updated, datetime) and last_updated > simulation_end:
            fail(
                f"last_updated {last_updated} is after the simulation horizon "
                f"{simulation_end}"
            )

        # The match's playtime bounds every artifact derived from it.
        if not isinstance(duration_sec, int) or duration_sec <= 0:
            fail(f"duration_sec must be a positive int, got {duration_sec!r}")
        elif duration_sec > match_duration_sec:
            fail(
                f"duration_sec {duration_sec}s exceeds the match window "
                f"{match_duration_sec}s (a clip cannot outrun its match)"
            )

        if content_type == "live":
            if live_start_at is None or live_end_at is None:
                fail(
                    "live artifact has no broadcast window "
                    "(live_start_at/live_end_at are None)"
                )
                continue
            if live_start_at != kickoff_at:
                fail(f"live_start_at {live_start_at} != kickoff_at {kickoff_at}")
            if live_end_at != match_end_at:
                fail(
                    f"live_end_at {live_end_at} != kickoff_at + match_duration_sec "
                    f"{match_end_at}"
                )
            # R5: created_at <= live_start_at
            if created_at > live_start_at:
                fail(
                    f"created_at {created_at} is after live_start_at "
                    f"{live_start_at} (the broadcast was published after kick-off)"
                )
            # R5: duration_sec consistent with the match window
            if isinstance(duration_sec, int) and duration_sec != match_duration_sec:
                fail(
                    f"live duration_sec {duration_sec}s != broadcast window "
                    f"{match_duration_sec}s"
                )
            # R5: live_start_at + duration_sec <= simulation_end
            if match_end_at > simulation_end:
                fail(
                    f"live window [{live_start_at}, {live_end_at}] exceeds the "
                    f"simulation horizon {simulation_end}"
                )
        else:
            if live_start_at is not None or live_end_at is not None:
                fail(
                    "VOD artifact must not have a broadcast window - the registry "
                    "serves VOD content from created_at onwards"
                )
            # A highlight/replay only exists after the match has been played.
            if created_at < match_end_at:
                fail(
                    f"created_at {created_at} precedes the final whistle "
                    f"{match_end_at} of the match it was cut from"
                )
            if (
                content_type == "replay"
                and isinstance(duration_sec, int)
                and duration_sec != match_duration_sec
            ):
                fail(
                    f"replay duration_sec {duration_sec}s != match window "
                    f"{match_duration_sec}s (a replay is the full match)"
                )

    if problems:
        preview = "; ".join(problems[:5])
        suffix = f" (+{len(problems) - 5} more)" if len(problems) > 5 else ""
        raise ValueError(
            f"{len(problems)} video artifact temporal integrity violation(s) (R5) "
            f"- check created_at <= live_start_at, live_start_at + duration_sec <= "
            f"simulation_end and that duration_sec matches the match window: "
            f"{preview}{suffix}"
        )