from datetime import datetime


class InteractionEntity:

    def __init__(
        self,
        *,
        event_id: str,
        event_timestamp: datetime,
        user_id: str,
        video_id: str,
        interaction_type: str,
        watch_time_sec: int | None,
        device_type: str,
    ):
        self.event_id = event_id
        self.event_timestamp = event_timestamp
        self.user_id = user_id
        self.video_id = video_id
        self.interaction_type = interaction_type
        self.watch_time_sec = watch_time_sec
        self.device_type = device_type

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "event_timestamp": self.event_timestamp,
            "user_id": self.user_id,
            "video_id": self.video_id,
            "interaction_type": self.interaction_type,
            "watch_time_sec": self.watch_time_sec,
            "device_type": self.device_type,
        }