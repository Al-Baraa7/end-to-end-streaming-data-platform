from copy import deepcopy
from datetime import timedelta


class PostgresDirtyDataInjector:

    def inject_users(self, users):
        users = deepcopy(users)

        if users:
            users[0].email = None

        if len(users) > 1:
            users[1].country = "XXX"

        if len(users) > 2:
            users[2].last_login = (
                users[2].created_at
                - timedelta(days=1)
            )

        if len(users) > 3 and users[3].email:
            users[3].email = f"  {users[3].email}  "

        return users

    def inject_subscriptions(self, subscriptions):
        subscriptions = deepcopy(subscriptions)

        if subscriptions:
            subscription = subscriptions[0]

            if subscription.end_date is not None:
                subscription.end_date = (
                    subscription.start_date
                    - timedelta(days=1)
                )

        return subscriptions

    def inject_payments(self, payments):
        return deepcopy(payments)

    def inject(
        self,
        *,
        users,
        subscriptions,
        payments,
    ):
        return (
            self.inject_users(users),
            self.inject_subscriptions(subscriptions),
            self.inject_payments(payments),
        )