

from datetime import timedelta

from scripts.simulation.id_generator import IDGenerator
from scripts.simulation.random_provider import RandomProvider
from scripts.simulation.postgres.payment_entity import PaymentEntity


class PaymentFactory:

    AMOUNTS = {
        30: 9.99,
        90: 14.99,
        365: 29.99,
    }

    CURRENCY = "USD"

    PAYMENT_STATUSES = (
        "success",
        "failed",
    )

    PAYMENT_STATUS_WEIGHTS = (
        0.85,
        0.15,
    )

    RETRY_DELAY_HOURS = (
        6,
        12,
        24,
        48,
    )

    def __init__(
        self,
        *,
        random_provider: RandomProvider,
    ):
        self.random = random_provider

    def create_attempt(
        self,
        *,
        subscription_id: str,
        subscription_duration_days: int,
        payment_date,
    ) -> PaymentEntity:

        if subscription_duration_days not in self.AMOUNTS:
            raise ValueError(
                "Unsupported subscription duration"
            )

        status = self.random.choices(
            self.PAYMENT_STATUSES,
            weights=self.PAYMENT_STATUS_WEIGHTS,
            k=1,
        )[0]

        return PaymentEntity(
            payment_id=IDGenerator.generate(),
            subscription_id=subscription_id,
            amount=self.AMOUNTS[
                subscription_duration_days
            ],
            currency=self.CURRENCY,
            payment_date=payment_date,
            payment_status=status,
        )

    def next_retry_date(self, *, payment_date):
        delay_hours = self.random.choice(
            self.RETRY_DELAY_HOURS
        )

        return (
            payment_date
            + timedelta(hours=delay_hours)
        )