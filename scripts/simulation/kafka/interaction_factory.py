"""Interaction Factory — Phase 3: Kafka Event Emission (Per-Event Assembly)

Builds individual InteractionEntity objects with:
- Random interaction type (play, pause, complete, like)
- Watch-time bounds per content type (R18)
- Device type assignment
- Timestamp from BusinessClock
See README.md#phase-markers-used-in-code-comments
"""

from datetime import datetime

from scripts.simulation.id_generator import IDGenerator
from scripts.simulation.random_provider import RandomProvider
from scripts.simulation.kafka.interaction_entity import InteractionEntity
from scripts.simulation.mongo.video_simulation import VideoSimulation


class InteractionFactory:

    INTERACTIONS = (
        "like",
        "play",
        "complete",
        "pause",
    )

    DEVICES = (
        "mobile",
        "tv",
        "web",
    )


    def __init__(
        self,
        *,
        clock,
        random_provider: RandomProvider,
    ):
        self.clock = clock
        self.random = random_provider

    def _get_watch_time_bounds(self, video_duration: int | None, content_type: str | None = None) -> tuple[int, int]:
        """Get watch-time bounds for a video based on content type and duration (R18)."""
        if content_type and content_type in VideoSimulation.WATCH_TIME_BOUNDS:
            min_wt, max_wt = VideoSimulation.WATCH_TIME_BOUNDS[content_type]
            if video_duration is not None:
                max_wt = min(max_wt, video_duration)
            return min_wt, max_wt
        # Fallback: legacy behavior (1-120s or video_duration)
        if video_duration is not None and video_duration > 1:
            return 1, min(120, video_duration - 1)
        return 1, 120

    
    # -- Phase 3: Event Assembly --

    def create(
        self,
        *,
        user_id: str,
        video_id: str,
        video_duration: int | None,
        content_type: str | None = None,
        event_timestamp: datetime | None = None,
    ) -> InteractionEntity:

        interaction_type = self.random.choice(
            self.INTERACTIONS
        )

        watch_time_sec = None

        if (
                interaction_type == "play"
                and video_duration is not None
                and video_duration > 1
            ):
                min_wt, max_wt = self._get_watch_time_bounds(video_duration, content_type)
                watch_time_sec = self.random.randint(min_wt, max_wt)

        elif (
                interaction_type == "complete"
                and video_duration is not None
                and video_duration > 0
            ):
                watch_time_sec = video_duration
       

        return InteractionEntity(
            event_id=IDGenerator.generate(),
            event_timestamp=(
                event_timestamp
                if event_timestamp is not None
                else self.clock.now()
            ),
            user_id=user_id,
            video_id=video_id,
            interaction_type=interaction_type,
            watch_time_sec=watch_time_sec,
            device_type=self.random.choice(
                self.DEVICES
        ),
    
   
        )

    def generate(
        self,
        *,
        user_ids,
        video_ids,
        count: int,
        video_durations: dict | None = None,
        content_types: dict | None = None,
    ):

        for _ in range(count):

            user_id = self.random.choice(
                user_ids
            )

            video_id = self.random.choice(
                video_ids
            )

            video_duration = video_durations.get(video_id) if video_durations else None
            content_type = content_types.get(video_id) if content_types else None

            yield self.create(
                user_id=user_id,
                video_id=video_id,
                video_duration=video_duration,
                content_type=content_type,
            )

            # Move simulation time forward.
            # We do not want thousands of events
            # occurring at the exact same instant.
            self.clock.advance(
                seconds=self.random.randint(
                    5,
                    120,
                )
            )