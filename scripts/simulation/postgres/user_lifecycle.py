"""User Lifecycle — Phase 8: Account Status Evaluation (R8)

Evaluates user lifecycle status (active/suspended/deleted) based on payment history.
Free-tier users are always ACTIVE; premium users transition on payment gaps.
See README.md#phase-markers-used-in-code-comments
"""

from datetime import datetime, timedelta


class UserLifecycle:

    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETED = "deleted"

    # ACTIVE -> SUSPENDED -> DELETED is the longest possible transition chain, so
    # replaying evaluate() more than this many times can never change the result.
    MAX_STATUS_TRANSITIONS = 2

    def __init__(
        self,
        *,
        suspension_after_days: int = 30,
        deletion_after_suspension_days: int = 7,
    ):
        if suspension_after_days <= 0:
            raise ValueError(
                "suspension_after_days must be greater than zero"
            )

        if deletion_after_suspension_days <= 0:
            raise ValueError(
                "deletion_after_suspension_days must be greater than zero"
            )

        self.suspension_after_days = suspension_after_days
        self.deletion_after_suspension_days = (
            deletion_after_suspension_days
        )

    def initial_status(self) -> str:
        return self.ACTIVE

    def evaluate(
        self,
        *,
        created_at: datetime,
        last_payment_at: datetime | None,
        current_status: str,
        now: datetime,
        has_premium_subscription: bool = False,
    ) -> str:
        """
        Evaluate user lifecycle status.

        CRITICAL: Free-tier users (has_premium_subscription=False) are ALWAYS
        returned as ACTIVE and are completely exempt from payment-gap suspension
        or deletion. Only premium users with failed/overdue payments can be
        suspended or deleted.
        """

        if now < created_at:
            raise ValueError(
                "Lifecycle cannot be evaluated before user creation"
            )

        # FREE USER PROTECTION: Free users can never be suspended or deleted
        # for non-payment. They remain ACTIVE regardless of payment history.
        if not has_premium_subscription:
            return self.ACTIVE

        if current_status == self.DELETED:
            return self.DELETED

        reference_time = last_payment_at or created_at

        days_without_payment = (
            now - reference_time
        ).days

        if current_status == self.ACTIVE:
            if days_without_payment >= self.suspension_after_days:
                return self.SUSPENDED

            return self.ACTIVE

        if current_status == self.SUSPENDED:
            suspension_start = reference_time + timedelta(
                days=self.suspension_after_days
            )

            if (
                now - suspension_start
            ).days >= self.deletion_after_suspension_days:
                return self.DELETED

            return self.SUSPENDED

        raise ValueError(
            f"Unknown account status: {current_status}"
        )

    def evaluate_until_stable(
        self,
        *,
        created_at: datetime,
        last_payment_at: datetime | None,
        current_status: str,
        now: datetime,
        has_premium_subscription: bool = False,
    ) -> str:
        """
        Replay evaluate() from the CURRENT status until it stops changing.

        evaluate() is deliberately path-dependent: a payment gap moves
        ACTIVE -> SUSPENDED, and only a SECOND evaluation can move
        SUSPENDED -> DELETED. Calling it once at SIMULATION_END would therefore
        leave every premium account that stopped paying long ago stuck in
        SUSPENDED forever, and the `deleted` state would never appear in the
        dataset.

        The loop is bounded by MAX_STATUS_TRANSITIONS + 1, so it always
        terminates; free-tier users short-circuit to ACTIVE on the first pass.
        """
        status = current_status

        for _ in range(self.MAX_STATUS_TRANSITIONS + 1):
            next_status = self.evaluate(
                created_at=created_at,
                last_payment_at=last_payment_at,
                current_status=status,
                now=now,
                has_premium_subscription=has_premium_subscription,
            )

            if next_status == status:
                return status

            status = next_status

        return status

    def compute_transition_dates(
        self,
        *,
        created_at: datetime,
        last_payment_at: datetime | None,
        has_premium_subscription: bool = False,
    ) -> tuple[datetime | None, datetime | None]:
        """
        Compute the effective suspension and deletion dates for a user.

        Returns (suspension_date, deletion_date) where:
        - suspension_date: when ACTIVE -> SUSPENDED transition occurs (or None if never)
        - deletion_date: when SUSPENDED -> DELETED transition occurs (or None if never)

        For free-tier users, both are None (always ACTIVE).
        """
        if not has_premium_subscription:
            return None, None

        reference_time = last_payment_at or created_at
        suspension_date = reference_time + timedelta(days=self.suspension_after_days)
        deletion_date = suspension_date + timedelta(days=self.deletion_after_suspension_days)

        return suspension_date, deletion_date