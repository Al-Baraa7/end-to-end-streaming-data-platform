

"""Subscription Factory — Phase 2: PostgreSQL Entity Generation (Subscriptions)

Generates subscription entities for PostgreSQL simulation.
Computes `is_active` against simulation horizon, not mutable clock cursor.
See README.md#phase-markers-used-in-code-comments
"""

from datetime import datetime, timedelta
from typing import Optional

from ..clock import BusinessClock
from ..id_generator import IDGenerator
from ..random_provider import RandomProvider
from .subscription_entity import SubscriptionEntity


class SubscriptionFactory:

    PLAN_TYPES = (
        "free",
        "premium",
    )

    FREE_DURATION_DAYS = (
        30,
        90,
    )

    PREMIUM_DURATIONS_DAYS = (
        30,
        90,
        365,
    )

    def __init__(
        self,
        *,
        random_provider: RandomProvider,
        clock: BusinessClock,
    ):
        self.random = random_provider
        self.clock = clock

    def create(
        self,
        *,
        user_id: str,
        start_date: datetime,
        plan_type: Optional[str] = None,
    ) -> SubscriptionEntity:

        if plan_type is None:
            plan_type = self.random.choice(self.PLAN_TYPES)

        if plan_type == "free":
            duration_days = self.random.choice(self.FREE_DURATION_DAYS)

            end_date = start_date + timedelta(days=duration_days)

            is_active = self._compute_is_active(start_date, end_date)

            return SubscriptionEntity(
                subscription_id=IDGenerator.generate(),
                user_id=user_id,
                plan_type="free",
                start_date=start_date,
                end_date=end_date,
                is_active=is_active,
                auto_renew=False,
                nominal_duration_days=duration_days,
            )

        duration_days = self.random.choice(self.PREMIUM_DURATIONS_DAYS)

        end_date = start_date + timedelta(days=duration_days)

        auto_renew = self.random.random() < 0.60

        is_active = self._compute_is_active(start_date, end_date)

        return SubscriptionEntity(
            subscription_id=IDGenerator.generate(),
            user_id=user_id,
            plan_type="premium",
            start_date=start_date,
            end_date=end_date,
            is_active=is_active,
            auto_renew=auto_renew,
            nominal_duration_days=duration_days,
        )

    def _compute_is_active(self, start_date: datetime, end_date: datetime) -> bool:
        """
        Compute is_active based on the simulation horizon (simulation_end),
        NOT the current mutable clock cursor. This ensures is_active reflects
        whether the subscription was active at any point during the simulation window,
        resolving the stale is_active bug where it was computed at generation-time
        (START_TIME) instead of simulation-end.
        """
        # Use simulation_end as the reference "now" for active status resolution
        # If simulation_end is None (unbounded), fall back to clock.now()
        reference_time = (
            self.clock.simulation_end
            if self.clock.simulation_end is not None
            else self.clock.now()
        )
        return start_date <= reference_time <= end_date