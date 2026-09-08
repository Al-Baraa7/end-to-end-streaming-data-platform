from datetime import datetime, timedelta, timezone


class BusinessClock:
    def __init__(self, start: datetime):
        if start.tzinfo is None:
            raise ValueError("BusinessClock start time must be timezone-aware")

        self._current_time = start.astimezone(timezone.utc)

    def now(self) -> datetime:
        return self._current_time

    def advance(
        self,
        *,
        seconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
        days: int = 0
    ) -> None:

        delta = timedelta(
            seconds=seconds,
            minutes=minutes,
            hours=hours,
            days=days,
        )

        if delta.total_seconds() < 0:
            raise ValueError("BusinessClock cannot move backwards")

        self._current_time += delta

    def set(self, value: datetime) -> None:
        if value.tzinfo is None:
            raise ValueError("BusinessClock time must be timezone-aware")

        value = value.astimezone(timezone.utc)

        if value < self._current_time:
            raise ValueError("BusinessClock cannot move backwards")

        self._current_time = value