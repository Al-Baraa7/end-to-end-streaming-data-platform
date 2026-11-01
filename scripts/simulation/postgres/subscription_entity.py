"""Subscription Entity — Phase 2: PostgreSQL Entity Generation (Subscription Schema)

Schema definition for a PostgreSQL subscription entity.
Tracks nominal duration vs actual window for R1 horizon clamping telemetry.
See README.md#phase-markers-used-in-code-comments
"""

from datetime import datetime


class SubscriptionEntity:

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
        # Duration of the PURCHASED plan (30 / 90 / 365 days), kept separate from
        # (end_date - start_date) on purpose: a subscription whose purchased window
        # overflows the simulation horizon is truncated at SIMULATION_END (R1), while
        # its billing price must still be derived from the purchased plan.
        # Intentionally NOT part of to_dict(): it is a generation-time attribute,
        # not a persisted column.
        self.nominal_duration_days = nominal_duration_days

    @property
    def is_truncated(self) -> bool:
        """True when the window was clamped at SIMULATION_END (R1 telemetry).

        A subscription always keeps its PURCHASED duration in
        `nominal_duration_days`, so a shorter (end_date - start_date) can only mean
        the simulation horizon cut the window short.
        """
        if self.nominal_duration_days is None or self.end_date is None:
            return False

        return (self.end_date - self.start_date).days < self.nominal_duration_days

    def to_dict(self) -> dict:
        return {
            "subscription_id": self.subscription_id,
            "user_id": self.user_id,
            "plan_type": self.plan_type,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "is_active": self.is_active,
            "auto_renew": self.auto_renew,
        }