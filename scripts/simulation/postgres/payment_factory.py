

"""Payment Factory — Phase 5: PostgreSQL Entity Generation (Payments)

Payment factory with a single plan/pricing table as the only source of amounts.

R15: Single plan/pricing table as the only source of amounts.
All amounts derive from PLAN_PRICING; no hard-coded amounts elsewhere.
See README.md#phase-markers-used-in-code-comments
"""

from datetime import timedelta

from ..id_generator import IDGenerator
from ..random_provider import RandomProvider
from .payment_entity import PaymentEntity


class PaymentFactory:
    """Payment factory with a single plan/pricing table as the only source of amounts.

    R15: Single plan/pricing table as the only source of amounts.
    All amounts derive from PLAN_PRICING; no hard-coded amounts elsewhere.
    """

    # R15: Single plan/pricing table as the only source of amounts
    PLAN_PRICING = {
        "free": {
            30: 0.00,
            90: 0.00,
        },
        "premium": {
            30: 9.99,
            90: 14.99,
            365: 29.99,
        },
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

    # Phase 5 - R18: Module-level constants for retry/attempt budgets
    MAX_ATTEMPTS = 3
    BILLING_REASONS = (
        "initial",
        "retry",
        "renewal",
    )

    def __init__(
        self,
        *,
        random_provider: RandomProvider,
    ):
        self.random = random_provider

    def get_amount(self, plan_type: str, duration_days: int) -> float:
        """Get amount from the single plan/pricing table (R15)."""
        if plan_type not in self.PLAN_PRICING:
            raise ValueError(f"Unknown plan_type: {plan_type}")
        if duration_days not in self.PLAN_PRICING[plan_type]:
            raise ValueError(f"Unsupported duration {duration_days} for plan {plan_type}")
        return self.PLAN_PRICING[plan_type][duration_days]

    def create_attempt(
        self,
        *,
        subscription_id: str,
        user_id: str,
        plan_type: str,
        subscription_duration_days: int,
        payment_date,
        attempt_no: int = 1,
        billing_reason: str = "initial",
    ) -> PaymentEntity:

        amount = self.get_amount(plan_type, subscription_duration_days)

        status = self.random.choices(
            self.PAYMENT_STATUSES,
            weights=self.PAYMENT_STATUS_WEIGHTS,
            k=1,
        )[0]

        # Phase 5 - R15: Validate attempt_no and billing_reason
        if attempt_no < 1 or attempt_no > self.MAX_ATTEMPTS:
            raise ValueError(f"attempt_no must be in [1, {self.MAX_ATTEMPTS}], got {attempt_no}")
        if billing_reason not in self.BILLING_REASONS:
            raise ValueError(f"billing_reason must be one of {self.BILLING_REASONS}, got {billing_reason}")

        return PaymentEntity(
            payment_id=IDGenerator.generate(),
            subscription_id=subscription_id,
            user_id=user_id,
            amount=amount,
            currency=self.CURRENCY,
            payment_date=payment_date,
            payment_status=status,
            attempt_no=attempt_no,
            billing_reason=billing_reason,
        )

    def next_retry_date(self, *, payment_date):
        delay_hours = self.random.choice(
            self.RETRY_DELAY_HOURS
        )

        return (
            payment_date
            + timedelta(hours=delay_hours)
        )