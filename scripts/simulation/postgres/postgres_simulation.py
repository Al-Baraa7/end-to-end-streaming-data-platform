
from scripts.simulation.id_generator import IDGenerator
from scripts.simulation.postgres.payment_factory import PaymentFactory
from scripts.simulation.random_provider import RandomProvider
from scripts.simulation.postgres.subscription_factory import SubscriptionFactory
from scripts.simulation.postgres.user_factory import UserFactory
from scripts.simulation.postgres.user_lifecycle import UserLifecycle
from scripts.simulation.postgres.user_simulation import UserSimulation


class PostgresSimulation:

    MIN_SUBSCRIPTIONS_PER_USER = 1
    MAX_SUBSCRIPTIONS_PER_USER = 3

    def __init__(
        self,
        *,
        clock,
        random_provider: RandomProvider,
    ):
        self.clock = clock
        self.random = random_provider

        self.user_simulation = UserSimulation(
            clock=clock,
            random_provider=random_provider,
        )

        self.user_factory = UserFactory(
            random_provider=random_provider,
        )

        self.user_lifecycle = UserLifecycle()

        self.subscription_factory = SubscriptionFactory(
            random_provider=random_provider,
        )

        self.payment_factory = PaymentFactory(
            random_provider=random_provider,
        )

    def generate_user(
        self,
        *,
        user_id: str | None = None,
    ):
        if user_id is None:
            user_id = IDGenerator.generate()

        return self.user_simulation.create_user()

    def generate_subscription(
        self,
        *,
        user_id: str,
    ):
        return self.subscription_factory.create(
            user_id=user_id,
            start_date=self.clock.now(),
        )

    def generate_payment_chain(
        self,
        *,
        subscription,
        max_attempts: int = 3,
    ):
        if subscription.plan_type == "free":
            return

        subscription_duration_days = (
            subscription.end_date
            - subscription.start_date
        ).days

        for _ in range(max_attempts):

            payment = self.payment_factory.create_attempt(
                subscription_id=subscription.subscription_id,
                subscription_duration_days=subscription_duration_days,
                payment_date=self.clock.now(),
            )

            yield payment

            if payment.payment_status == "success":
                return

            retry_date = self.payment_factory.next_retry_date(
                payment_date=payment.payment_date,
            )

            delta = retry_date - self.clock.now()

            if delta.total_seconds() < 0:
                raise ValueError(
                    "Payment retry cannot move BusinessClock backwards"
                )

            self.clock.advance(
                seconds=int(delta.total_seconds())
            )

    def generate_user_lifecycle(
        self,
        *,
        user_id: str | None = None,
    ):
        user = self.generate_user(
            user_id=user_id,
        )

        yield {
            "type": "user_created",
            "data": user,
        }

        subscription_count = self.random.randint(
            self.MIN_SUBSCRIPTIONS_PER_USER,
            self.MAX_SUBSCRIPTIONS_PER_USER,
        )

        for _ in range(subscription_count):

            subscription = self.generate_subscription(
                user_id=user.user_id,
            )

            yield {
                "type": "subscription_created",
                "data": subscription,
            }

            for payment in self.generate_payment_chain(
                subscription=subscription,
            ):
                yield {
                    "type": "payment_created",
                    "data": payment,
                }

            if subscription.end_date is not None:
                self.clock.set(
                    subscription.end_date
                )

    def generate_users(
        self,
        *,
        count: int,
    ):
        for _ in range(count):
            yield self.generate_user()