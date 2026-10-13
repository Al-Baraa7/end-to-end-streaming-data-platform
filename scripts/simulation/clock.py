from datetime import datetime, timedelta, timezone


class BusinessClock:
    def __init__(
        self,
        start: datetime,
        *,
        simulation_end: datetime | None = None
    ):
        if start.tzinfo is None:
            raise ValueError("BusinessClock start time must be timezone-aware")

        if simulation_end is not None:
            if simulation_end.tzinfo is None:
                raise ValueError("BusinessClock simulation_end must be timezone-aware")
            simulation_end = simulation_end.astimezone(timezone.utc)
            if simulation_end <= start:
                raise ValueError("simulation_end must be after start time")

        self._current_time = start.astimezone(timezone.utc)
        self._simulation_end = simulation_end
        self._start_time = self._current_time

    @property
    def start_time(self) -> datetime:
        return self._start_time

    @property
    def simulation_end(self) -> datetime | None:
        return self._simulation_end

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

        new_time = self._current_time + delta

        if self._simulation_end is not None and new_time > self._simulation_end:
            raise ValueError(
                f"BusinessClock advance would exceed simulation horizon: "
                f"{new_time} > {self._simulation_end}"
            )

        self._current_time = new_time

    def advance_to(self, target: datetime) -> None:
        """
        Advance clock to a target time (backwards allowed).
        Supports multi-phase historical entity backfilling by allowing
        the clock to move backwards to a user's registration time.
        Still respects the simulation_end horizon constraint.
        """
        if target.tzinfo is None:
            raise ValueError("Target time must be timezone-aware")

        target = target.astimezone(timezone.utc)

        # Allow moving backwards for multi-phase backfilling
        # Only enforce the simulation_end horizon constraint
        if self._simulation_end is not None and target > self._simulation_end:
            raise ValueError(
                f"Target time {target} exceeds simulation horizon {self._simulation_end}"
            )

        self._current_time = target

    def is_before_horizon(self, timestamp: datetime) -> bool:
        """Check if a given timestamp falls within the simulation horizon."""
        if self._simulation_end is None:
            return True
        if timestamp.tzinfo is None:
            raise ValueError("Timestamp must be timezone-aware")
        return timestamp.astimezone(timezone.utc) <= self._simulation_end

    def remaining_duration(self) -> timedelta | None:
        """Return remaining time until simulation_end, or None if unbounded."""
        if self._simulation_end is None:
            return None
        remaining = self._simulation_end - self._current_time
        return remaining if remaining.total_seconds() > 0 else timedelta(0)