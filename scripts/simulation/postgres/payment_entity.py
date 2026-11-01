"""Payment Entity — Phase 5: PostgreSQL Entity Generation (Payment Schema)

Schema definition for a PostgreSQL payment entity.
Includes R15 fields: attempt_no, billing_reason.
See README.md#phase-markers-used-in-code-comments
"""

from datetime import datetime


class PaymentEntity:

    def __init__(
        self,
        *,
        payment_id: str,
        subscription_id: str,
        user_id: str,
        amount: float,
        currency: str,
        payment_date: datetime,
        payment_status: str,
        attempt_no: int = 1,
        billing_reason: str = "initial",
    ):
        self.payment_id = payment_id
        self.subscription_id = subscription_id
        self.user_id = user_id
        self.amount = amount
        self.currency = currency
        self.payment_date = payment_date
        self.payment_status = payment_status
        # Phase 5 - R15 Gold DDL Change: attempt_no + billing_reason on PaymentEntity
        self.attempt_no = attempt_no
        self.billing_reason = billing_reason

    def to_dict(self) -> dict:
        return {
            "payment_id": self.payment_id,
            "subscription_id": self.subscription_id,
            "user_id": self.user_id,
            "amount": self.amount,
            "currency": self.currency,
            "payment_date": self.payment_date,
            "payment_status": self.payment_status,
            "attempt_no": self.attempt_no,
            "billing_reason": self.billing_reason,
        }