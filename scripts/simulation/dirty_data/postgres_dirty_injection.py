"""PostgreSQL Dirty Data Injector — Phase 7: Anomaly Injection (PostgreSQL)

Uses a shared RandomProvider for stratified, deterministic anomaly injection
at a volume-weighted blended 3.5% rate (strictly bounded 1%-5% overall)
across all entity types. The coordinator enforces global budget.

Applies deepcopy() micro-mutations on individual entity objects ONLY after
they pass clean verification. The pristine master object remains untouched
in memory for subsequent Kafka session/event telemetry.
See README.md#phase-markers-used-in-code-comments
"""

from copy import deepcopy
from datetime import timedelta
from typing import Iterator, List

from ..random_provider import RandomProvider

# Valid country codes from UserFactory
VALID_COUNTRIES = ("KSA", "UAE", "EGY", "KWT", "QAT", "MAR", "JOR")
VALID_ACCOUNT_STATUSES = ("active", "suspended", "deleted")
VALID_PLAN_TYPES = ("free", "premium")
VALID_PAYMENT_STATUSES = ("success", "failed", "refunded")
VALID_BILLING_REASONS = ("initial", "retry", "renewal")


class PostgresDirtyDataInjector:
    """
    Streaming row-by-row dirty data injector for PostgreSQL entities.
    
    Uses a SHARED RandomProvider (injected by coordinator) to inject anomalies
    at rates calculated by the global volume coordinator to achieve a
    volume-weighted blended 3.5% rate (bounded 1%-5% overall).
    Each entity is deep-copied before mutation to preserve the pristine master.
    """

    def __init__(self, random_provider: RandomProvider):
        """
        Initialize with a SHARED RandomProvider for deterministic anomaly injection.
        
        Args:
            random_provider: Shared RandomProvider instance from GlobalAnomalyCoordinator.
        """
        self.random = random_provider

    def _should_inject(self, anomaly_rate: float) -> bool:
        """Determine if an anomaly should be injected based on the rate."""
        return self.random.random() < anomaly_rate

    def _inject_user_anomalies(self, user) -> None:
        """Inject anomalies into a single user entity (mutates the deepcopy)."""
        anomaly_type = self.random.randint(1, 5)

        if anomaly_type == 1:
            # Null mandatory field: email
            user.email = None
        elif anomaly_type == 2:
            # Invalid domain enum: country
            user.country = "XXX"
        elif anomaly_type == 3:
            # Temporal skew: last_login < created_at
            if user.created_at and user.last_login:
                user.last_login = user.created_at - timedelta(days=self.random.randint(1, 30))
        elif anomaly_type == 4:
            # Invalid account_status enum (now allowed with VARCHAR)
            user.account_status = "INVALID_STATUS"
        elif anomaly_type == 5:
            # Empty email (whitespace only)
            user.email = "   "

    def _inject_subscription_anomalies(self, subscription) -> None:
        """Inject anomalies into a single subscription entity (mutates the deepcopy)."""
        anomaly_type = self.random.randint(1, 2)

        if anomaly_type == 1:
            # Invalid plan_type enum
            subscription.plan_type = "INVALID_PLAN"
        elif anomaly_type == 2:
            # Temporal skew: end_date < start_date
            if subscription.start_date and subscription.end_date:
                subscription.end_date = subscription.start_date - timedelta(days=self.random.randint(1, 30))
        # DISABLED: Invalid is_active (non-boolean string)
        # elif anomaly_type == 3:
        #     subscription.is_active = "not_a_boolean"
        # DISABLED: Referential orphan: user_id mutation
        # elif anomaly_type == 3:
        #     subscription.user_id = ""

    def _inject_payment_anomalies(self, payment) -> None:
        """Inject anomalies into a single payment entity (mutates the deepcopy)."""
        anomaly_type = self.random.randint(1, 6)

        if anomaly_type == 1:
            # Invalid payment_status enum (now allowed with VARCHAR)
            payment.payment_status = "INVALID_STATUS"
        elif anomaly_type == 2:
            # Invalid billing_reason enum (now allowed with VARCHAR)
            payment.billing_reason = "INVALID_REASON"
        elif anomaly_type == 3:
            # Valid but potentially wrong payment_status (breaks business logic)
            # Randomly assign a valid status that may not match payment reality
            payment.payment_status = self.random.choice(VALID_PAYMENT_STATUSES)
        elif anomaly_type == 4:
            # Valid but potentially wrong billing_reason (breaks business logic)
            # Randomly assign a valid reason that may not match payment context
            payment.billing_reason = self.random.choice(VALID_BILLING_REASONS)
        elif anomaly_type == 5:
            # Attempt_no out of bounds (should be 1-3)
            payment.attempt_no = self.random.randint(4, 10)
        elif anomaly_type == 6:
            # Negative amount
            payment.amount = -abs(payment.amount)
        # DISABLED: anomaly_type == 7 - Referential orphan: subscription_id/user_id mutation
        # elif anomaly_type == 7:
        #     # Referential orphan: subscription_id or user_id that doesn't exist
        #     if self.random.random() < 0.5:
        #         payment.subscription_id = ""
        #     else:
        #         payment.user_id = ""

    def inject_users(self, users: List, anomaly_rate: float) -> Iterator:
        """
        Stream users with anomalies injected at the coordinator-calculated rate.
        
        Yields deep-copied entities (pristine or corrupted) one at a time
        to avoid memory inflation. Pristine master objects remain untouched.
        """
        for user in users:
            user_copy = deepcopy(user)

            if self._should_inject(anomaly_rate):
                self._inject_user_anomalies(user_copy)

            yield user_copy

    def inject_subscriptions(self, subscriptions: List, anomaly_rate: float) -> Iterator:
        """Stream subscriptions with anomalies injected at the coordinator-calculated rate."""
        for subscription in subscriptions:
            subscription_copy = deepcopy(subscription)

            if self._should_inject(anomaly_rate):
                self._inject_subscription_anomalies(subscription_copy)

            yield subscription_copy

    def inject_payments(self, payments: List, anomaly_rate: float) -> Iterator:
        """Stream payments with anomalies injected at the coordinator-calculated rate."""
        for payment in payments:
            payment_copy = deepcopy(payment)

            if self._should_inject(anomaly_rate):
                self._inject_payment_anomalies(payment_copy)

            yield payment_copy

    def inject(
        self,
        *,
        users,
        subscriptions,
        payments,
        user_anomaly_rate: float,
        subscription_anomaly_rate: float,
        payment_anomaly_rate: float,
    ):
        """
        Inject anomalies across all PostgreSQL entity types using coordinator rates.
        
        Returns tuples of iterators for streaming processing.
        The pristine master lists remain untouched in memory.
        """
        return (
            self.inject_users(users, user_anomaly_rate),
            self.inject_subscriptions(subscriptions, subscription_anomaly_rate),
            self.inject_payments(payments, payment_anomaly_rate),
        )