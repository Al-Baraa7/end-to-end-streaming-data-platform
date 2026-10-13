

from datetime import timedelta

from scripts.simulation.id_generator import IDGenerator
from scripts.simulation.random_provider import RandomProvider
from scripts.simulation.postgres.subscription_entity import SubscriptionEntity


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
    ):
        self.random = random_provider

    def create(
        self,
        *,
        user_id: str,
        start_date,
    ) -> SubscriptionEntity:

        plan_type = self.random.choice(
            self.PLAN_TYPES
        )

        if plan_type == "free":
            duration_days = self.random.choice(
                self.FREE_DURATION_DAYS
            )

            end_date = (
                start_date
                + timedelta(days=duration_days)
            )

            return SubscriptionEntity(
                subscription_id=IDGenerator.generate(),
                user_id=user_id,
                plan_type="free",
                start_date=start_date,
                end_date=end_date,
                is_active=True,
                auto_renew=False,
            )

        duration_days = self.random.choice(
            self.PREMIUM_DURATIONS_DAYS
        )

        end_date = (
            start_date
            + timedelta(days=duration_days)
        )

        auto_renew = (
            self.random.random() < 0.60
        )

        return SubscriptionEntity(
            subscription_id=IDGenerator.generate(),
            user_id=user_id,
            plan_type="premium",
            start_date=start_date,
            end_date=end_date,
            is_active=True,
            auto_renew=auto_renew,
        )