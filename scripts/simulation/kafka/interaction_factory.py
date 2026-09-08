from datetime import datetime

from scripts.simulation.id_generator import IDGenerator
from scripts.simulation.random_provider import RandomProvider
from scripts.simulation.kafka.interaction_entity import InteractionEntity


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

    # WATCH_TIME_RANGES = {
    #     "play": (5, 120),
    #     "complete": (30, 7200),
    # }

    def __init__(
        self,
        *,
        clock,
        random_provider: RandomProvider,
    ):
        self.clock = clock
        self.random = random_provider

    
    def create(
        self,
        *,
        user_id: str,
        video_id: str,
        video_duration: int | None,
        event_timestamp: datetime | None = None,
    ) -> InteractionEntity:

        interaction_type = self.random.choice(
            self.INTERACTIONS
        )

        watch_time_sec = None

        if (
                interaction_type == "play"
                and video_duration is not None
                and video_duration > 0
            ):
                watch_time_sec = self.random.randint(
                    1,
                    min(120, video_duration),
                )

        elif (
                interaction_type == "complete"
                and video_duration is not None
                and video_duration > 0
            ):
                watch_time_sec = self.random.randint(
                    max(1, video_duration - 60),
                    video_duration,
                )
        # if interaction_type == "play" and video_duration is not None:
        #     watch_time_sec = self.random.randint(
        #         1,
        #         min(120, video_duration),
        #     )

        # elif interaction_type == "complete" and video_duration is not None:
        #     watch_time_sec = self.random.randint(
        #         max(1, video_duration - 60),
        #         video_duration,
        #     )

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
    
    #     self,
    #     *,
    #     user_id: str,
    #     video_id: str,
    #     event_timestamp: datetime | None = None,
    # ) -> InteractionEntity:

    #     interaction_type = self.random.choice(
    #         self.INTERACTIONS
    #     )

    #     watch_time_sec = None

    #     if interaction_type in self.WATCH_TIME_RANGES:
    #         minimum, maximum = (
    #             self.WATCH_TIME_RANGES[
    #                 interaction_type
    #             ]
    #         )

    #         watch_time_sec = self.random.randint(
    #             minimum,
    #             maximum,
    #         )

    #     return InteractionEntity(
    #         event_id=IDGenerator.generate(),
    #         event_timestamp=event_timestamp if event_timestamp is not None else self.clock.now(),
    #         user_id=user_id,
    #         video_id=video_id,
    #         interaction_type=interaction_type,
    #         watch_time_sec=watch_time_sec,
    #         device_type=self.random.choice(
    #             self.DEVICES
    #         ),
        )

    def generate(
        self,
        *,
        user_ids,
        video_ids,
        count: int,
    ):

        for _ in range(count):

            user_id = self.random.choice(
                user_ids
            )

            video_id = self.random.choice(
                video_ids
            )

            yield self.create(
                user_id=user_id,
                video_id=video_id,
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