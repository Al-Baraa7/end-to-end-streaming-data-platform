from datetime import datetime, timedelta


class UserLifecycle:

    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETED = "deleted"

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
    ) -> str:

        if now < created_at:
            raise ValueError(
                "Lifecycle cannot be evaluated before user creation"
            )

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