from datetime import timedelta

from scripts.simulation.clock import BusinessClock
from scripts.simulation.random_provider import RandomProvider
from scripts.simulation.postgres.user_entity import UserEntity
from scripts.simulation.postgres.user_factory import UserFactory
from scripts.simulation.postgres.user_lifecycle import UserLifecycle


class UserSimulation:

    def __init__(
        self,
        *,
        clock: BusinessClock,
        random_provider: RandomProvider,
        lifecycle: UserLifecycle | None = None,
    ):
        self.clock = clock
        self.random = random_provider

        self.factory = UserFactory(
            random_provider=self.random
        )

        self.lifecycle = (
            lifecycle
            if lifecycle is not None
            else UserLifecycle()
        )

    def create_user(self) -> UserEntity:
        user = UserEntity(
            user_id=self.factory.generate_user_id(),
            email=self.factory.generate_email(),
            country=self.factory.generate_country(),
            account_status=self.lifecycle.initial_status(),
            created_at=self.clock.now(),
            last_login=None,
        )

        return user

    def login_user(
        self,
        *,
        user: UserEntity,
        days_after_creation: int,
    ) -> UserEntity:

        if days_after_creation < 0:
            raise ValueError(
                "Login cannot happen before user creation"
            )

        login_time = (
            user.created_at
            + timedelta(days=days_after_creation)
        )

        if login_time < self.clock.now():
            raise ValueError(
                "Login time cannot be before current simulation time"
            )

        if login_time > self.clock.now():
            delta = login_time - self.clock.now()

            self.clock.advance(
                seconds=int(delta.total_seconds())
            )

        user.last_login = login_time

        return user

    def evaluate_lifecycle(
        self,
        *,
        user: UserEntity,
        last_payment_at=None,
    ) -> UserEntity:

        user.account_status = self.lifecycle.evaluate(
            created_at=user.created_at,
            last_payment_at=last_payment_at,
            current_status=user.account_status,
            now=self.clock.now(),
        )

        return user