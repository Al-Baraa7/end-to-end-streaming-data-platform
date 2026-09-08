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
    ):
        self.subscription_id = subscription_id
        self.user_id = user_id
        self.plan_type = plan_type
        self.start_date = start_date
        self.end_date = end_date
        self.is_active = is_active
        self.auto_renew = auto_renew

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