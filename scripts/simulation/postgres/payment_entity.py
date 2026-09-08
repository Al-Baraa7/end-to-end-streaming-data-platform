from datetime import datetime


class PaymentEntity:

    def __init__(
        self,
        *,
        payment_id: str,
        subscription_id: str,
        amount: float,
        currency: str,
        payment_date: datetime,
        payment_status: str,
    ):
        self.payment_id = payment_id
        self.subscription_id = subscription_id
        self.amount = amount
        self.currency = currency
        self.payment_date = payment_date
        self.payment_status = payment_status

    def to_dict(self) -> dict:
        return {
            "payment_id": self.payment_id,
            "subscription_id": self.subscription_id,
            "amount": self.amount,
            "currency": self.currency,
            "payment_date": self.payment_date,
            "payment_status": self.payment_status,
        }