"""TemporalRegistry — Phase 1: Temporal Isolation (R7)

Immutable eligibility map for Kafka event sampling. Loads paid subscriptions + active users + available videos.
Computes composite `is_valid_event()` gate. See README.md#phase-markers-used-in-code-comments
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SubscriptionInterval:
    """Immutable subscription validity window."""
    user_id: str
    start: datetime
    end: datetime


@dataclass(frozen=True, slots=True)
class VideoWindow:
    """Immutable video availability window."""
    video_id: str
    created_at: datetime
    live_start_at: Optional[datetime]
    live_end_at: Optional[datetime]


class TemporalRegistry:
    """
    Validates referential integrity across Postgres (subscriptions) and MongoDB (videos)
    at a specific point in simulated time.

    Usage:
        registry = TemporalRegistry()
        registry.load_from_memory(subscriptions, videos)   # during simulation
        # OR
        registry.load_from_postgres(pg_pool)               # downstream
        registry.load_from_mongodb(mongo_client)

        if registry.is_valid_event(user_id, video_id, event_ts):
            # event is temporally valid
    """

    def __init__(self, reference_tz: timezone = timezone.utc):
        """
        Args:
            reference_tz: Timezone to normalize all timestamps to.
                          Defaults to UTC (matches BusinessClock).
        """
        self._reference_tz = reference_tz
        self._subscription_intervals: Dict[str, List[SubscriptionInterval]] = {}
        self._video_windows: Dict[str, VideoWindow] = {}
        self._loaded = False

        # R7 eligibility gate (see load_from_memory). None == "no gating", so the
        # registry keeps working exactly as before for callers that do not model
        # payments/account status (downstream ETL, tests, notebooks).
        self._paid_subscription_ids: Optional[Set[str]] = None
        self._active_user_ids: Optional[Set[str]] = None
        self._gated_out_subscriptions = 0

    # -- Phase 1: Loading API --

    def load_from_memory(
        self,
        subscriptions: List[dict],
        videos: List[dict],
        *,
        paid_subscription_ids: Optional[Iterable[str]] = None,
        active_user_ids: Optional[Iterable[str]] = None,
    ) -> None:
        """
        Load intervals from in-memory simulation objects (dicts with datetime fields).

        Expected subscription dict keys: user_id, start_date, end_date, subscription_id
        Expected video dict keys: video_id, created_at, live_start_at, live_end_at

        R7 eligibility gate (optional; both default to "no gating"):

        * `paid_subscription_ids` — only windows whose `subscription_id` appears
          here are loaded, so the registry can never serve a window that was not
          actually PAID FOR (R6: a premium window without a successful payment).
        * `active_user_ids` — only these users can pass
          `user_has_active_subscription()`, so an account the R8 lifecycle
          evaluation suspended or deleted can no longer consume content, no matter
          how many (paid) windows it still owns.

        Gating happens at LOAD time, not per probe, so the hot path in the event
        loop stays a plain interval scan.
        """
        self._subscription_intervals.clear()
        self._video_windows.clear()

        self._paid_subscription_ids = (
            set(paid_subscription_ids) if paid_subscription_ids is not None else None
        )
        self._active_user_ids = (
            set(active_user_ids) if active_user_ids is not None else None
        )
        self._gated_out_subscriptions = 0

        for sub in subscriptions:
            if (
                self._paid_subscription_ids is not None
                and sub.get("subscription_id") not in self._paid_subscription_ids
            ):
                # R7: unpaid (or rejected) window - never loaded, so it can never
                # be sampled. Counted so the loss is observable, never silent.
                self._gated_out_subscriptions += 1
                continue

            user_id = sub["user_id"]
            start = self._normalize(sub["start_date"])
            end = self._normalize(sub["end_date"])
            interval = SubscriptionInterval(user_id=user_id, start=start, end=end)
            self._subscription_intervals.setdefault(user_id, []).append(interval)

        # Sort intervals per user for binary-search-style lookup (optional optimization)
        for intervals in self._subscription_intervals.values():
            intervals.sort(key=lambda i: i.start)

        for vid in videos:
            v_id = vid["video_id"]
            created = self._normalize(vid["created_at"])
            live_start = self._normalize(vid["live_start_at"]) if vid.get("live_start_at") else None
            live_end = self._normalize(vid["live_end_at"]) if vid.get("live_end_at") else None
            self._video_windows[v_id] = VideoWindow(
                video_id=v_id,
                created_at=created,
                live_start_at=live_start,
                live_end_at=live_end,
            )

        self._loaded = True

    def load_from_postgres(self, pg_pool) -> None:
        """
        Load subscription intervals from Postgres.
        Implement when downstream consumers need it.
        """
        raise NotImplementedError("Wire this when downstream ETL needs direct DB access")

    def load_from_mongodb(self, mongo_client) -> None:
        """
        Load video windows from MongoDB.
        Implement when downstream consumers need it.
        """
        raise NotImplementedError("Wire this when downstream ETL needs direct DB access")

    # -- Phase 1: Core Validation API --

    def user_has_active_subscription(self, user_id: str, ts: datetime) -> bool:
        """
        Return True if the user has at least one subscription active at timestamp ts.

        R7: when an active-user gate was supplied to `load_from_memory()`, an
        account that is no longer `active` (suspended/deleted by the R8 lifecycle
        evaluation) returns False even if a paid window covers ts.
        """
        self._ensure_loaded()

        if self._active_user_ids is not None and user_id not in self._active_user_ids:
            return False

        ts = self._normalize(ts)

        intervals = self._subscription_intervals.get(user_id)
        if not intervals:
            return False

        # Intervals are sorted by start; linear scan is fine for typical counts (< 5/user)
        for interval in intervals:
            if interval.start <= ts <= interval.end:
                return True
            if interval.start > ts:
                # Since sorted, no later interval can match
                break
        return False

    def video_is_available(self, video_id: str, ts: datetime) -> bool:
        """
        Return True if the video exists and is available at timestamp ts.

        Availability rules:
        - VOD (highlight/replay): available from created_at onwards
        - Live: available only between live_start_at and live_end_at (inclusive)
        """
        self._ensure_loaded()
        ts = self._normalize(ts)

        window = self._video_windows.get(video_id)
        if not window:
            return False

        # Video must be created
        if ts < window.created_at:
            return False

        # Live content has explicit broadcast window
        if window.live_start_at is not None and window.live_end_at is not None:
            return window.live_start_at <= ts <= window.live_end_at

        # VOD content: available after creation indefinitely
        return True

    def is_valid_event(self, user_id: str, video_id: str, ts: datetime) -> bool:
        """
        Single composite check: user has active subscription AND video is available at ts.
        """
        return (
            self.user_has_active_subscription(user_id, ts)
            and self.video_is_available(video_id, ts)
        )

    # -- Phase 1: Introspection / Debugging --

    def get_user_subscription_count(self, user_id: str) -> int:
        """Number of subscription intervals loaded for a user.

        R7-gated windows are not loaded at all, so they are not counted.
        """
        return len(self._subscription_intervals.get(user_id, []))

    @property
    def gated_out_subscription_count(self) -> int:
        """R7 telemetry: windows rejected by the paid-subscription gate at load time."""
        return self._gated_out_subscriptions

    @property
    def active_user_ids(self) -> Optional[Set[str]]:
        """The R7 account-status gate, or None when gating is disabled."""
        return self._active_user_ids

    def get_video_window(self, video_id: str) -> Optional[VideoWindow]:
        """Retrieve the availability window for a video (for debugging)."""
        return self._video_windows.get(video_id)

    @property
    def video_windows(self) -> Dict[str, VideoWindow]:
        """
        All loaded video availability windows (read-only copy).

        Lets callers reason about EVERY video at once - e.g. the R13 invariant that
        each video has at least one timestamp this registry can actually serve -
        instead of probing one id at a time.
        """
        self._ensure_loaded()
        return dict(self._video_windows)

    def is_loaded(self) -> bool:
        return self._loaded

    # -- Phase 1: Internal Helpers --

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            raise RuntimeError(
                "TemporalRegistry not loaded. Call load_from_memory() first."
            )

    def _normalize(self, dt: Optional[datetime]) -> Optional[datetime]:
        """
        Ensure datetime is timezone-aware in the reference timezone.
        Naive datetimes are treated as UTC.
        """
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=self._reference_tz)
        if dt.tzinfo != self._reference_tz:
            return dt.astimezone(self._reference_tz)
        return dt