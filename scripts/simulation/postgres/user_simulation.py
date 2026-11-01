"""User Simulation — Phase 2/8: PostgreSQL User Lifecycle (R1/R8)

Orchestrates user creation, login, and R8 lifecycle evaluation.
Uses per-user clock forks (R1) to avoid mutating the master cursor.
See README.md#phase-markers-used-in-code-comments
"""

from datetime import datetime, timedelta

from ..clock import BusinessClock
from ..random_provider import RandomProvider
from .user_entity import UserEntity
from .user_factory import UserFactory
from .user_lifecycle import UserLifecycle


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
        clock: BusinessClock | None = None,
    ) -> UserEntity:
        """
        Record the user's most recent login instant.

        `clock` defaults to this simulation's master cursor (standalone usage). The
        live orchestrator passes a per-user FORK anchored at the user's creation
        time, so the login lands inside the user's own timeline while the master
        cursor is never mutated (R1).
        """

        active_clock = clock if clock is not None else self.clock

        if days_after_creation < 0:
            raise ValueError(
                "Login cannot happen before user creation"
            )

        login_time = (
            user.created_at
            + timedelta(days=days_after_creation)
        )

        if login_time < active_clock.now():
            raise ValueError(
                "Login time cannot be before current simulation time"
            )

        if login_time > active_clock.now():
            delta = login_time - active_clock.now()

            active_clock.advance(
                seconds=int(delta.total_seconds())
            )

        user.last_login = login_time

        return user

    def evaluate_lifecycle(
        self,
        *,
        user: UserEntity,
        last_payment_at=None,
        has_premium_subscription: bool = False,
        now: datetime | None = None,
    ) -> UserEntity:
        """
        Evaluate (and backfill) the user's account status.

        `now` defaults to the clock cursor; the live orchestrator evaluates at
        SIMULATION_END so the persisted `account_status` describes the account at
        the end of the horizon (R8) rather than at whichever instant the master
        cursor happened to stop on.

        Uses `lifecycle.evaluate_until_stable()` so long payment gaps can reach
        the terminal `deleted` state within a single call.
        """

        user.account_status = self.lifecycle.evaluate_until_stable(
            created_at=user.created_at,
            last_payment_at=last_payment_at,
            current_status=user.account_status,
            now=now if now is not None else self.clock.now(),
            has_premium_subscription=has_premium_subscription,
        )

        return user