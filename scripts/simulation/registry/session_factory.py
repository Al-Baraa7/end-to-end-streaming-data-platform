"""Session Factory — Phase 4: Session Topology & Event Clustering

Replaces the per-video independent Poisson draws with a session-centric model:
- A session is a coherent viewing episode: one `play` (the session start) followed by
  zero or more subsequent interactions (pause, complete, like) drawn from a
  within-session distribution.
- `watch_time_sec` = dwell between consecutive event timestamps, capped by the
  content duration (VOD) or live window length (live).
- Sessions cluster around live kickoff ± window for live matches and evening
  prime-time for VOD/replay.
- Per-user device/engagement tiers modulate session frequency and like probability.
- Like ratio: derived from LIKE_RATIO_MIN/MAX (0.5-3%) of coupled views, then
  reconciled so `stats.likes == like_events` and `stats.views == play_events`.
See README.md#phase-markers-used-in-code-comments
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple
from enum import Enum

from ..random_provider import RandomProvider
from ..id_generator import IDGenerator
from ..kafka.interaction_entity import InteractionEntity
from ..mongo.video_simulation import VideoSimulation
from ..temporal_registry import TemporalRegistry
# ---------------------------------------------------------------------------
# Configurable Constants (can be overridden via config.py / CLI in future)
# ---------------------------------------------------------------------------

# Session frequency tiers (sessions per user per year)
ENGAGEMENT_TIERS = {
    "high": {"weight": 0.15, "sessions_per_year": 24, "like_ratio_range": (0.02, 0.03)},
    "medium": {"weight": 0.50, "sessions_per_year": 12, "like_ratio_range": (0.01, 0.02)},
    "low": {"weight": 0.35, "sessions_per_year": 4, "like_ratio_range": (0.005, 0.01)},
}

# Device tier distribution per engagement tier
DEVICE_TIER_DIST = {
    "high": {"mobile": 0.40, "tv": 0.45, "web": 0.15},
    "medium": {"mobile": 0.55, "tv": 0.30, "web": 0.15},
    "low": {"mobile": 0.70, "tv": 0.15, "web": 0.15},
}

# Live clustering: sessions concentrate around kickoff
LIVE_CLUSTER_WINDOW_MINUTES = 30      # ±30 min around kickoff
LIVE_CLUSTER_FRACTION = 0.70          # 70% of live sessions in cluster window
LIVE_PRE_KICKOFF_MINUTES = 15         # sessions can start up to 15 min before kickoff

# VOD prime-time clustering (local evening 19:00-23:00)
VOD_PRIME_START_HOUR = 19
VOD_PRIME_END_HOUR = 23
VOD_PRIME_FRACTION = 0.60             # 60% of VOD sessions in prime time

# Within-session interaction probabilities (conditional on previous event)
SESSION_INTERACTION_PROBS = {
    "high": {
        "play->pause": 0.35,
        "play->complete": 0.25,
        "play->like": 0.08,
        "pause->play": 0.60,
        "pause->complete": 0.15,
        "pause->like": 0.05,
    },
    "medium": {
        "play->pause": 0.25,
        "play->complete": 0.20,
        "play->like": 0.05,
        "pause->play": 0.50,
        "pause->complete": 0.12,
        "pause->like": 0.03,
    },
    "low": {
        "play->pause": 0.15,
        "play->complete": 0.15,
        "play->like": 0.02,
        "pause->play": 0.40,
        "pause->complete": 0.10,
        "pause->like": 0.01,
    },
}

# Dwell time distributions (seconds) between consecutive events
DWELL_DISTRIBUTIONS = {
    "play->pause": (30, 300),      # 30s - 5min
    "pause->play": (10, 120),      # 10s - 2min
    "play->complete": (0, 0),      # complete ends session, no dwell
    "play->like": (5, 60),         # like shortly after play
    "pause->complete": (0, 0),
    "pause->like": (5, 60),
}

# Maximum events per session (prevents runaway sessions)
MAX_EVENTS_PER_SESSION = 8

# Minimum session gap (prevents overlapping sessions for same user)
MIN_SESSION_GAP_MINUTES = 5


class EngagementTier(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(frozen=True, slots=True)
class UserSessionProfile:
    """Per-user session behavior profile (assigned once at user creation)."""
    user_id: str
    engagement_tier: EngagementTier
    sessions_per_year: int
    like_ratio_min: float
    like_ratio_max: float
    device_distribution: Dict[str, float]
    interaction_probs: Dict[str, float]


@dataclass(frozen=True, slots=True)
class Session:
    """A coherent viewing session: one play + subsequent interactions."""
    session_id: str
    user_id: str
    video_id: str
    start_ts: datetime
    end_ts: datetime
    content_type: str
    device_type: str
    engagement_tier: EngagementTier
    events: List[InteractionEntity] = field(default_factory=list)
    
    @property
    def event_count(self) -> int:
        return len(self.events)
    
    @property
    def has_play(self) -> bool:
        return any(e.interaction_type == "play" for e in self.events)
    
    @property
    def like_count(self) -> int:
        return sum(1 for e in self.events if e.interaction_type == "like")
    
    @property
    def play_count(self) -> int:
        return sum(1 for e in self.events if e.interaction_type == "play")


@dataclass(frozen=True, slots=True)
class SessionPlan:
    """Pre-computed session plan for a user (before event generation)."""
    user_id: str
    video_id: str
    content_type: str
    start_ts: datetime
    device_type: str
    engagement_tier: EngagementTier
    target_event_count: int  # Poisson draw from session length distribution
class SessionFactory:
    """
    Generates session-centric event streams.
    
    Usage:
        factory = SessionFactory(
            clock=event_clock,
            random_provider=random_provider,
            registry=temporal_registry,
            videos=videos,
            user_profiles=user_profiles,
        )
        sessions = factory.generate_sessions_for_user(user_id, eligible_windows)
        events = factory.emit_session_events(sessions)
    """
    
    def __init__(
        self,
        *,
        clock,
        random_provider: RandomProvider,
        registry: TemporalRegistry,
        videos: List[dict],
        user_profiles: Dict[str, UserSessionProfile],
        like_ratio_min: float = 0.005,
        like_ratio_max: float = 0.03,
    ):
        self.clock = clock
        self.random = random_provider
        self.registry = registry
        self.videos = videos
        self.user_profiles = user_profiles
        self.like_ratio_min = like_ratio_min
        self.like_ratio_max = like_ratio_max
        
        self.video_map = {v["video_id"]: v for v in videos}
        self.live_videos = [v for v in videos if v.get("content_type") == "live"]
        self.vod_videos = [v for v in videos if v.get("content_type") in ("highlight", "replay")]
    
    @classmethod
    def assign_user_profiles(
        cls,
        user_ids: List[str],
        random_provider: RandomProvider,
    ) -> Dict[str, UserSessionProfile]:
        profiles = {}
        for user_id in user_ids:
            tier_choice = random_provider.choices(
                list(ENGAGEMENT_TIERS.keys()),
                weights=[ENGAGEMENT_TIERS[t]["weight"] for t in ENGAGEMENT_TIERS],
                k=1
            )[0]
            tier_config = ENGAGEMENT_TIERS[tier_choice]
            
            profiles[user_id] = UserSessionProfile(
                user_id=user_id,
                engagement_tier=EngagementTier(tier_choice),
                sessions_per_year=tier_config["sessions_per_year"],
                like_ratio_min=tier_config["like_ratio_range"][0],
                like_ratio_max=tier_config["like_ratio_range"][1],
                device_distribution=DEVICE_TIER_DIST[tier_choice],
                interaction_probs=SESSION_INTERACTION_PROBS[tier_choice],
            )
        return profiles

    def generate_session_plan(
        self,
        user_id: str,
        video_id: str,
        simulation_start: datetime,
        simulation_end: datetime,
    ) -> Optional[SessionPlan]:
        profile = self.user_profiles.get(user_id)
        if not profile:
            return None
        
        video = self.video_map.get(video_id)
        if not video:
            return None
        
        content_type = video.get("content_type", "replay")
        
        video_window = self.registry.get_video_window(video_id)
        if not video_window:
            return None
        
        if video_window.live_start_at and video_window.live_end_at:
            avail_start = video_window.live_start_at
            avail_end = video_window.live_end_at
        else:
            avail_start = video_window.created_at
            avail_end = simulation_end
        
        if avail_start >= avail_end:
            return None
        
        start_ts = self._sample_session_start(
            content_type=content_type,
            avail_start=avail_start,
            avail_end=avail_end,
            video=video,
        )
        
        if not start_ts or start_ts < avail_start or start_ts > avail_end:
            return None
        
        if not self.registry.user_has_active_subscription(user_id, start_ts):
            return None
        
        if not self.registry.video_is_available(video_id, start_ts):
            return None
        
        device_type = self.random.choices(
            list(profile.device_distribution.keys()),
            weights=list(profile.device_distribution.values()),
            k=1
        )[0]
        
        target_events = self._sample_session_length(profile.engagement_tier)
        
        return SessionPlan(
            user_id=user_id,
            video_id=video_id,
            content_type=content_type,
            start_ts=start_ts,
            device_type=device_type,
            engagement_tier=profile.engagement_tier,
            target_event_count=target_events,
        )

    def _validate_watch_time_bounds(self, content_type: str, watch_time_sec: int | None, max_duration: int) -> int | None:
        """Validate and clamp watch_time_sec against content-type bounds (R18)."""
        if watch_time_sec is None:
            return None
        bounds = VideoSimulation.WATCH_TIME_BOUNDS.get(content_type)
        if bounds:
            min_wt, max_wt = bounds
            # Clamp to content-type bounds, then to max_duration
            max_wt = min(max_wt, max_duration) if max_duration > 0 else max_wt
            return max(min_wt, min(watch_time_sec, max_wt))
        return watch_time_sec

    def _sample_session_start(
        self,
        content_type: str,
        avail_start: datetime,
        avail_end: datetime,
        video: dict,
    ) -> Optional[datetime]:
        window_seconds = int((avail_end - avail_start).total_seconds())
        if window_seconds <= 0:
            return None
        
        if content_type == "live":
            kickoff = video.get("kickoff_at") or video.get("live_start_at")
            if kickoff:
                if self.random.random() < LIVE_CLUSTER_FRACTION:
                    cluster_start = kickoff - timedelta(minutes=LIVE_PRE_KICKOFF_MINUTES)
                    cluster_end = kickoff + timedelta(minutes=LIVE_CLUSTER_WINDOW_MINUTES)
                    
                    cluster_start = max(cluster_start, avail_start)
                    cluster_end = min(cluster_end, avail_end)
                    
                    if cluster_start < cluster_end:
                        cluster_seconds = int((cluster_end - cluster_start).total_seconds())
                        offset = self.random.randint(0, cluster_seconds)
                        return cluster_start + timedelta(seconds=offset)
            
            offset = self.random.randint(0, window_seconds)
            return avail_start + timedelta(seconds=offset)
        
        else:
            prime_start = avail_start.replace(hour=VOD_PRIME_START_HOUR, minute=0, second=0, microsecond=0)
            prime_end = avail_start.replace(hour=VOD_PRIME_END_HOUR, minute=0, second=0, microsecond=0)
            
            prime_start = max(prime_start, avail_start)
            prime_end = min(prime_end, avail_end)
            
            if prime_start < prime_end and self.random.random() < VOD_PRIME_FRACTION:
                prime_seconds = int((prime_end - prime_start).total_seconds())
                offset = self.random.randint(0, prime_seconds)
                return prime_start + timedelta(seconds=offset)
            
            offset = self.random.randint(0, window_seconds)
            return avail_start + timedelta(seconds=offset)
    
    def _sample_session_length(self, tier: EngagementTier) -> int:
        if tier == EngagementTier.HIGH:
            return min(MAX_EVENTS_PER_SESSION, max(1, int(self.random.gauss(4, 1.5) + 0.5)))
        elif tier == EngagementTier.MEDIUM:
            return min(MAX_EVENTS_PER_SESSION, max(1, int(self.random.gauss(3, 1.2) + 0.5)))
        else:
            return min(MAX_EVENTS_PER_SESSION, max(1, int(self.random.gauss(2, 1.0) + 0.5)))

    def emit_session_events(self, plan: SessionPlan) -> Session:
        profile = self.user_profiles[plan.user_id]
        video = self.video_map[plan.video_id]
        video_duration = video.get("duration_sec", 0)
        video_window = self.registry.get_video_window(plan.video_id)
        if plan.content_type == "live" and video_window and video_window.live_start_at and video_window.live_end_at:
            max_duration = int((video_window.live_end_at - video_window.live_start_at).total_seconds())
        else:
            max_duration = video_duration
        events = []
        current_ts = plan.start_ts
        play_event = InteractionEntity(
            event_id=IDGenerator.generate(),
            event_timestamp=current_ts,
            user_id=plan.user_id,
            video_id=plan.video_id,
            interaction_type="play",
            watch_time_sec=None,
            device_type=plan.device_type,
        )
        events.append(play_event)
        prev_type = "play"
        events_generated = 1
        while events_generated < plan.target_event_count:
            probs = profile.interaction_probs
            next_options = []
            next_weights = []
            stop_prob = 0.15
            if prev_type == "pause":
                stop_prob = 0.25
            if prev_type == "play":
                if self.random.random() < probs.get("play->pause", 0):
                    next_options.append("pause")
                    next_weights.append(probs["play->pause"])
                if self.random.random() < probs.get("play->complete", 0):
                    next_options.append("complete")
                    next_weights.append(probs["play->complete"])
                if self.random.random() < probs.get("play->like", 0):
                    next_options.append("like")
                    next_weights.append(probs["play->like"])
            elif prev_type == "pause":
                if self.random.random() < probs.get("pause->play", 0):
                    next_options.append("play")
                    next_weights.append(probs["pause->play"])
                if self.random.random() < probs.get("pause->complete", 0):
                    next_options.append("complete")
                    next_weights.append(probs["pause->complete"])
                if self.random.random() < probs.get("pause->like", 0):
                    next_options.append("like")
                    next_weights.append(probs["pause->like"])
            if not next_options:
                break
            total_w = sum(next_weights)
            if total_w == 0:
                break
            next_options.append("stop")
            next_weights.append(stop_prob * total_w)
            next_type = self.random.choices(next_options, weights=next_weights, k=1)[0]
            if next_type == "stop":
                complete_event = InteractionEntity(
                    event_id=IDGenerator.generate(),
                    event_timestamp=current_ts,
                    user_id=plan.user_id,
                    video_id=plan.video_id,
                    interaction_type="complete",
                    watch_time_sec=0,
                    device_type=plan.device_type,
                )
                events.append(complete_event)
                if len(events) >= 2 and events[-2].interaction_type == "play":
                    events[-2].watch_time_sec = 0
                break
            dwell_key = f"{prev_type}->{next_type}"
            dwell_min, dwell_max = DWELL_DISTRIBUTIONS.get(dwell_key, (5, 60))
            dwell = self.random.randint(dwell_min, dwell_max)
            current_ts = current_ts + timedelta(seconds=dwell)
            if not self.clock.is_before_horizon(current_ts):
                break
            if not self.registry.video_is_available(plan.video_id, current_ts):
                break
            if not self.registry.user_has_active_subscription(plan.user_id, current_ts):
                break
            elapsed = int((current_ts - plan.start_ts).total_seconds())
            if max_duration > 0 and elapsed >= max_duration:
                complete_event = InteractionEntity(
                    event_id=IDGenerator.generate(),
                    event_timestamp=current_ts,
                    user_id=plan.user_id,
                    video_id=plan.video_id,
                    interaction_type="complete",
                    watch_time_sec=0,
                    device_type=plan.device_type,
                )
                events.append(complete_event)
                if len(events) >= 2 and events[-2].interaction_type == "play":
                    # Apply content-type watch-time bounds (R18)
                    max_dur = self._validate_watch_time_bounds(plan.content_type, max_duration, max_duration)
                    events[-2].watch_time_sec = max_dur
                break
            event = InteractionEntity(
                event_id=IDGenerator.generate(),
                event_timestamp=current_ts,
                user_id=plan.user_id,
                video_id=plan.video_id,
                interaction_type=next_type,
                watch_time_sec=None,
                device_type=plan.device_type,
            )
            events.append(event)
            if prev_type in ("play", "pause") and len(events) >= 2 and events[-2].interaction_type in ("play", "pause"):
                # Apply content-type watch-time bounds (R18)
                dwell = self._validate_watch_time_bounds(plan.content_type, dwell, max_duration)
                events[-2].watch_time_sec = dwell
            prev_type = next_type
            events_generated += 1
        if events and events[-1].interaction_type in ("play", "pause"):
            elapsed = int((current_ts - plan.start_ts).total_seconds())
            if events[-1].interaction_type == "play":
                # Apply content-type watch-time bounds (R18)
                elapsed = self._validate_watch_time_bounds(plan.content_type, elapsed, max_duration)
                events[-1].watch_time_sec = min(elapsed, max_duration) if max_duration > 0 else elapsed

        session = Session(
            session_id=IDGenerator.generate(),
            user_id=plan.user_id,
            video_id=plan.video_id,
            start_ts=plan.start_ts,
            end_ts=current_ts,
            content_type=plan.content_type,
            device_type=plan.device_type,
            engagement_tier=plan.engagement_tier,
            events=events,
        )

        return session

    def generate_user_sessions(
        self,
        user_id: str,
        simulation_start: datetime,
        simulation_end: datetime,
        target_session_count: int,
    ) -> List[Session]:
        profile = self.user_profiles.get(user_id)
        if not profile:
            return []

        sessions = []
        last_session_end = simulation_start

        video_weights = []
        video_ids = []
        for video in self.videos:
            video_ids.append(video["video_id"])
            video_weights.append(float(video.get("latent_popularity", 1.0)))

        total_weight = sum(video_weights)
        if total_weight == 0:
            video_weights = [1.0] * len(video_weights)
            total_weight = len(video_weights)

        for _ in range(target_session_count):
            video_id = self.random.choices(video_ids, weights=video_weights, k=1)[0]

            plan = self.generate_session_plan(
                user_id=user_id,
                video_id=video_id,
                simulation_start=simulation_start,
                simulation_end=simulation_end,
            )

            if not plan:
                continue

            if plan.start_ts < last_session_end + timedelta(minutes=MIN_SESSION_GAP_MINUTES):
                plan = SessionPlan(
                    user_id=plan.user_id,
                    video_id=plan.video_id,
                    content_type=plan.content_type,
                    start_ts=last_session_end + timedelta(minutes=MIN_SESSION_GAP_MINUTES),
                    device_type=plan.device_type,
                    engagement_tier=plan.engagement_tier,
                    target_event_count=plan.target_event_count,
                )

                if not self.registry.user_has_active_subscription(user_id, plan.start_ts):
                    continue
                if not self.registry.video_is_available(plan.video_id, plan.start_ts):
                    continue
                if not self.clock.is_before_horizon(plan.start_ts):
                    continue

            session = self.emit_session_events(plan)
            if session.events:
                sessions.append(session)
                last_session_end = session.end_ts

        return sessions


def compute_like_ratio_target(
    total_views: int,
    like_ratio_min: float = 0.005,
    like_ratio_max: float = 0.03,
) -> int:
    ratio = (like_ratio_min + like_ratio_max) / 2.0
    return max(0, int(total_views * ratio))


def reconcile_likes_with_stats(
    sessions: List[Session],
    videos: List[dict],
    like_ratio_min: float = 0.005,
    like_ratio_max: float = 0.03,
) -> Tuple[List[Session], List[dict]]:
    total_plays = sum(s.play_count for s in sessions)
    total_likes = sum(s.like_count for s in sessions)

    target_likes = compute_like_ratio_target(total_plays, like_ratio_min, like_ratio_max)

    min_likes = max(0, int(total_plays * like_ratio_min))
    max_likes = max(min_likes, int(total_plays * like_ratio_max))
    target_likes = max(min_likes, min(max_likes, target_likes))

    delta = target_likes - total_likes

    if delta == 0:
        return sessions, videos

    tier_order = {EngagementTier.HIGH: 0, EngagementTier.MEDIUM: 1, EngagementTier.LOW: 2}
    sessions_sorted = sorted(sessions, key=lambda s: tier_order.get(s.engagement_tier, 3))

    if delta > 0:
        for session in sessions_sorted:
            if delta <= 0:
                break
            if session.play_count > 0 and session.like_count == 0:
                for i, event in enumerate(session.events):
                    if event.interaction_type == "play" and i + 1 < len(session.events):
                        next_event = session.events[i + 1]
                        if next_event.interaction_type in ("pause", "complete"):
                            if next_event.interaction_type == "pause":
                                next_event.interaction_type = "like"
                                next_event.watch_time_sec = 0
                                delta -= 1
                                break

    elif delta < 0:
        for session in reversed(sessions_sorted):
            if delta >= 0:
                break
            for event in session.events:
                if event.interaction_type == "like" and delta < 0:
                    event.interaction_type = "pause"
                    delta += 1

    video_like_counts = {}
    video_play_counts = {}

    for session in sessions:
        video_play_counts[session.video_id] = video_play_counts.get(session.video_id, 0) + session.play_count
        video_like_counts[session.video_id] = video_like_counts.get(session.video_id, 0) + session.like_count

    for video in videos:
        vid = video["video_id"]
        views = video_play_counts.get(vid, 0)
        likes = video_like_counts.get(vid, 0)
        if likes > views:
            likes = max(0, views - 1)
        video["stats"] = {"views": views, "likes": likes}

    return sessions, videos