
"""PostgreSQL Simulation — Phase 2/5/6/7/8: PostgreSQL Entity Lifecycle (R1/R6/R7/R8)

Orchestrates PostgreSQL entity generation: users, subscriptions, payments.
Handles R1 horizon clamping, R6 payment activation gate, R7 eligibility derivation,
and R8 lifecycle evaluation. Integrates with BillingEligibility for R6/R7.
See README.md#phase-markers-used-in-code-comments
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from ..clock import BusinessClock
from ..id_generator import IDGenerator
from .payment_factory import PaymentFactory
from ..random_provider import RandomProvider
from .subscription_factory import SubscriptionFactory
from .user_factory import UserFactory
from .user_lifecycle import UserLifecycle
from .user_simulation import UserSimulation
from .subscription_entity import SubscriptionEntity
from .payment_entity import PaymentEntity
from .user_entity import UserEntity
from ..registry.billing_eligibility import BillingEligibility


@dataclass
class UserJourneyContext:
    """In-memory state tracker for a single user's lifecycle generation."""
    user: UserEntity
    subscriptions: List[SubscriptionEntity] = field(default_factory=list)
    payments: List[PaymentEntity] = field(default_factory=list)
    last_successful_payment_at: Optional[datetime] = None
    suspension_date: Optional[datetime] = None
    deletion_date: Optional[datetime] = None

    @property
    def truncated_subscription_count(self) -> int:
        """How many of this user's windows were clamped at the horizon (R1)."""
        return sum(1 for subscription in self.subscriptions if subscription.is_truncated)

    def latest_allowed_login(self) -> Optional[datetime]:
        """
        Returns the latest timestamp a user could have logged in, based on
        their account lifecycle transitions.

        - If suspended: capped at suspension_date
        - If deleted: capped at deletion_date
        - If active (free or premium with payments): None (no cap, up to horizon)
        """
        if self.deletion_date is not None:
            return self.deletion_date
        if self.suspension_date is not None:
            return self.suspension_date
        return None


class PostgresSimulation:

    MIN_SUBSCRIPTIONS_PER_USER = 1
    MAX_SUBSCRIPTIONS_PER_USER = 3
    SUB_GAP_DAYS_MIN = 1
    SUB_GAP_DAYS_MAX = 30

    # Payment retry model. Single source of truth shared with the orchestrator, so
    # the horizon slack below and the actual retry count cannot drift apart.
    PAYMENT_MAX_ATTEMPTS = 3

    # Minimum slack that must remain before SIMULATION_END for a NEW subscription to
    # be allowed: a payment chain issues at most (attempts - 1) retries, the longest
    # of which is max(RETRY_DELAY_HOURS) = 48h. Keeping this slack guarantees that
    # every retry timestamp stays inside the simulation horizon, so the child clock's
    # BusinessClock.advance() can never raise mid-generation (R1).
    MIN_SUB_START_HORIZON_SLACK_HOURS = (
        max(PaymentFactory.RETRY_DELAY_HOURS) * (PAYMENT_MAX_ATTEMPTS - 1)
    )

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
            clock=clock,
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
        start_date: datetime | None = None,
        plan_type: Optional[str] = None,
    ):
        return self.subscription_factory.create(
            user_id=user_id,
            start_date=start_date if start_date is not None else self.clock.now(),
            plan_type=plan_type,
        )

    def generate_payment_chain(
        self,
        *,
        subscription: SubscriptionEntity,
        user_id: str,
        max_attempts: int = PAYMENT_MAX_ATTEMPTS,
        payment_clock: BusinessClock | None = None,
        first_billing_reason: str = "initial",
    ):
        """Generate payment attempts for a single billing event (initial, retry, or renewal).
        Returns list of payments and the final successful payment date (or None).
        
        Uses the provided payment_clock (forked child clock) for temporal isolation.
        If not provided, falls back to self.clock (legacy behavior).
        
        Phase 5 - R15: Uses single plan/pricing table; adds attempt_no + billing_reason.
        Phase 5 - R18: Uses module-level MAX_ATTEMPTS constant from PaymentFactory.
        
        Args:
            first_billing_reason: "initial" for first subscription, "renewal" for
                auto-renewed subscriptions. Subsequent retries always use "retry".
        """
        if subscription.plan_type == "free":
            return [], None

        # Price the subscription by the PURCHASED plan duration (R1): a subscription
        # whose window was truncated at SIMULATION_END keeps its nominal duration, so
        # billing amounts stay on the official price list (30 / 90 / 365).
        subscription_duration_days = (
            getattr(subscription, "nominal_duration_days", None)
            or (subscription.end_date - subscription.start_date).days
        )

        # Use the forked payment clock for this subscription's timeline
        clock = payment_clock if payment_clock is not None else self.clock

        payments = []
        for attempt_no in range(1, max_attempts + 1):
            # Phase 5 - R15: Determine billing_reason (initial vs retry vs renewal)
            if attempt_no == 1:
                billing_reason = first_billing_reason
            else:
                billing_reason = "retry"

            payment = self.payment_factory.create_attempt(
                subscription_id=subscription.subscription_id,
                user_id=user_id,
                plan_type=subscription.plan_type,
                subscription_duration_days=subscription_duration_days,
                payment_date=clock.now(),
                attempt_no=attempt_no,
                billing_reason=billing_reason,
            )

            payments.append(payment)

            if payment.payment_status == "success":
                return payments, payment.payment_date

            retry_date = self.payment_factory.next_retry_date(
                payment_date=payment.payment_date,
            )

            # R1 horizon guard: a retry dated after SIMULATION_END can no longer
            # happen, so the chain ends here instead of letting the child clock's
            # BusinessClock.advance() raise mid-generation.
            if not clock.is_before_horizon(retry_date):
                break

            delta = retry_date - clock.now()

            if delta.total_seconds() < 0:
                raise ValueError(
                    "Payment retry cannot move BusinessClock backwards"
                )

            clock.advance(
                seconds=int(delta.total_seconds())
            )

        return payments, None

    def generate_user_subscriptions(
        self,
        *,
        user: UserEntity,
        subscription_count: int,
        clock: BusinessClock | None = None,
    ) -> List[SubscriptionEntity]:
        """Generate one user's subscriptions as a STRICTLY SEQUENTIAL timeline (R2).

        Guarantees
        ----------
        * No overlap: every subscription starts after the previous one has ended,
          separated by SUB_GAP_DAYS_MIN..SUB_GAP_DAYS_MAX days
          (start_{i+1} = end_i + gap_days).
        * The first subscription starts exactly at user.created_at.
        * Horizon safety (R1): a subscription whose purchased window overflows the
          simulation horizon is truncated at SIMULATION_END, and no further
          subscription is created for that user (the remaining horizon cannot host
          one). Truncated subscriptions keep their purchased duration in
          `nominal_duration_days`, so their billing price is unchanged.
        * Temporal isolation (R1): start dates are explicit and the passed `clock`
          (normally a per-user fork) is used only as the horizon reference — the
          master clock cursor is never mutated by this method.
        """
        horizon_clock = clock if clock is not None else self.clock
        simulation_end = horizon_clock.simulation_end

        subscriptions: List[SubscriptionEntity] = []
        sub_start = user.created_at
        generated_count = 0

        while generated_count < subscription_count:
            if simulation_end is not None:
                # A new subscription may only start if its full payment chain
                # (attempts + retry back-offs) still fits inside the horizon.
                latest_start = simulation_end - timedelta(
                    hours=self.MIN_SUB_START_HORIZON_SLACK_HOURS
                )
                if sub_start > latest_start:
                    break

            subscription = self.generate_subscription(
                user_id=user.user_id,
                start_date=sub_start,
            )

            truncated = False
            if simulation_end is not None and subscription.end_date > simulation_end:
                # R1: never persist a subscription window beyond the horizon.
                subscription.end_date = simulation_end
                truncated = True

            subscriptions.append(subscription)
            generated_count += 1

            if truncated:
                # The horizon has been reached: nothing else can fit after it.
                break

            # Dynamic renewal decision: check if we should continue the chain.
            # Conditions for renewal:
            # 1. Current subscription has auto_renew == True
            # 2. There is horizon space for another subscription (checked at loop top)
            # 3. Account status proxy: current subscription's payment chain must succeed
            #    (we don't have payment results yet at this stage, so this check
            #    will be enforced in generate_user_journey by interleaving generation
            #    and payment processing). For this method, we only check auto_renew.
            if not subscription.auto_renew:
                break

            gap_days = self.random.randint(
                self.SUB_GAP_DAYS_MIN,
                self.SUB_GAP_DAYS_MAX,
            )
            sub_start = subscription.end_date + timedelta(days=gap_days)

        return subscriptions

    def _generate_subscriptions_sequential(
        self,
        *,
        context: UserJourneyContext,
        subscription_count: int,
    ) -> None:
        """Generate sequential (non-overlapping) subscriptions for a user.

        Thin adapter over generate_user_subscriptions() so the lifecycle path
        (generate_user_lifecycle) and the live orchestrator share exactly ONE
        implementation of the R2 ordering rule.
        """
        context.subscriptions.extend(
            self.generate_user_subscriptions(
                user=context.user,
                subscription_count=subscription_count,
                clock=self.clock.fork(start_at=context.user.created_at),
            )
        )

    def _process_payments_for_subscriptions(
        self,
        *,
        context: UserJourneyContext,
    ) -> None:
        """Process payment chains for all subscriptions, tracking last successful payment.
        
        Uses child clocks (forked from the global clock) to isolate each subscription's
        payment timeline. This prevents global clock mutation and enables parallel/
        deterministic payment generation.
        """
        for subscription in context.subscriptions:
            # Fork a child clock anchored at this subscription's start date.
            # The child clock shares the simulation_end horizon but has its own cursor.
            payment_clock = self.clock.fork(start_at=subscription.start_date)

            payments, success_date = self.generate_payment_chain(
                subscription=subscription,
                user_id=context.user.user_id,
                payment_clock=payment_clock,
            )

            for payment in payments:
                context.payments.append(payment)

            if success_date is not None:
                context.last_successful_payment_at = success_date

    def _evaluate_and_update_user_status(
        self,
        *,
        context: UserJourneyContext,
        # Evaluation instant. Defaults to SIMULATION_END (_horizon_reference) so the
        # persisted account_status is the END-OF-HORIZON state (R8).
        now: datetime | None = None,
    ) -> None:
        """Evaluate user lifecycle status based on payment history and subscription plan.

        Scans the user's generated subscription history to determine if they have
        ever held a premium plan. Free-tier users are mathematically exempt from
        payment-based suspension/deletion.

        Also computes and stores the effective suspension/deletion dates for use
        by `last_login` capping (prevents ghost logins on dead accounts).
        """
        evaluation_time = now if now is not None else self._horizon_reference()
        has_premium = any(
            sub.plan_type == "premium" for sub in context.subscriptions
        )

        # Evaluate and update account status
        self.user_simulation.evaluate_lifecycle(
            user=context.user,
            last_payment_at=context.last_successful_payment_at,
            has_premium_subscription=has_premium,
            now=evaluation_time,
        )

        # Compute transition dates for last_login capping
        suspension_date, deletion_date = self.user_lifecycle.compute_transition_dates(
            created_at=context.user.created_at,
            last_payment_at=context.last_successful_payment_at,
            has_premium_subscription=has_premium,
        )

        context.suspension_date = suspension_date
        context.deletion_date = deletion_date

    def _horizon_reference(self) -> datetime:
        """The instant lifecycle/payment gates are resolved against.

        SIMULATION_END when the run has a horizon (the normal case), otherwise the
        current cursor. Centralised so every R6/R8 decision in this class and in
        the orchestrator resolves at the SAME instant.
        """
        return (
            self.clock.simulation_end
            if self.clock.simulation_end is not None
            else self.clock.now()
        )

    def generate_user_journey(
        self,
        *,
        user: UserEntity,
        subscription_count: int,
        now: datetime | None = None,
    ) -> UserJourneyContext:
        """Build ONE user's complete Postgres journey in the canonical order:

            subscriptions (R2) -> payment chains (R1) -> account status (R8)
            -> last_login (R8)

        This is the single per-user entry point, used by the live orchestrator
        (run_simulation.py step [5]) AND by the generate_user_lifecycle() event
        facade, so the two paths cannot drift (19b: same subscription generator,
        same payment chain, same lifecycle rules).

        Renewal logic (auto_renew integration):
        - First subscription uses billing_reason="initial"
        - Subsequent subscriptions (renewals) only generated if:
            * Previous subscription.auto_renew == True
            * Previous subscription's payment chain had at least one success
            * Horizon space remains for another subscription + payment chain
        - Renewal subscriptions use billing_reason="renewal" for first attempt
        - Retries always use billing_reason="retry"

        All temporal work happens on forked child clocks, so the master cursor is
        never mutated (R1). `now` defaults to the simulation horizon.
        """
        reference = now if now is not None else self._horizon_reference()

        context = UserJourneyContext(user=user)

        # Fork a clock for this user's subscription timeline (R1 temporal isolation)
        sub_clock = self.clock.fork(start_at=user.created_at)
        simulation_end = sub_clock.simulation_end
        sub_start = user.created_at
        generated_count = 0
        is_first_subscription = True

        while generated_count < subscription_count:
            # Horizon guard: check if we have space for a new subscription + payment chain
            if simulation_end is not None:
                latest_start = simulation_end - timedelta(
                    hours=self.MIN_SUB_START_HORIZON_SLACK_HOURS
                )
                if sub_start > latest_start:
                    break

            # Generate the next subscription
            subscription = self.generate_subscription(
                user_id=user.user_id,
                start_date=sub_start,
            )

            truncated = False
            if simulation_end is not None and subscription.end_date > simulation_end:
                subscription.end_date = simulation_end
                truncated = True

            context.subscriptions.append(subscription)
            generated_count += 1

            # Process payment chain for THIS subscription immediately
            # (interleaved generation for proper renewal decisions)
            payment_clock = self.clock.fork(start_at=subscription.start_date)
            
            # First subscription: "initial", Renewals: "renewal"
            first_billing_reason = "initial" if is_first_subscription else "renewal"
            
            payments, success_date = self.generate_payment_chain(
                subscription=subscription,
                user_id=user.user_id,
                payment_clock=payment_clock,
                first_billing_reason=first_billing_reason,
            )

            for payment in payments:
                context.payments.append(payment)

            if success_date is not None:
                context.last_successful_payment_at = success_date

            # If truncated at horizon, no more subscriptions can fit
            if truncated:
                break

            # Renewal decision: check if we should continue the chain
            # Conditions:
            # 1. Current subscription has auto_renew == True
            # 2. Payment chain had at least one success (account still active proxy)
            # 3. Horizon space (checked at loop top)
            if not subscription.auto_renew:
                break
            if success_date is None:
                # No successful payment -> user would be suspended/deleted per R8
                # Stop the renewal chain
                break

            # Advance to next subscription start date
            gap_days = self.random.randint(
                self.SUB_GAP_DAYS_MIN,
                self.SUB_GAP_DAYS_MAX,
            )
            sub_start = subscription.end_date + timedelta(days=gap_days)
            is_first_subscription = False

        # Final lifecycle evaluation at horizon (R8)
        self._evaluate_and_update_user_status(context=context, now=reference)
        self._record_user_login(context=context, now=reference)

        return context

    def _record_user_login(
        self,
        *,
        context: UserJourneyContext,
        now: datetime | None = None,
    ) -> None:
        """R8 - populate `user.last_login`.

        The login instant is drawn anywhere in [created_at, min(horizon, latest_allowed_login)]
        and applied to a child clock FORKED at the user's creation time, so the master cursor is
        never mutated (R1) while login_user()'s monotonicity check still holds
        inside the user's own timeline.

        `latest_allowed_login` caps the login at the earliest of:
        - suspension_date (if user becomes suspended)
        - deletion_date (if user becomes deleted)
        - horizon (if user remains active throughout)

        This prevents the logical paradox where a suspended/deleted user would have
        a `last_login` timestamp AFTER their account was closed.
        """
        horizon = now if now is not None else self._horizon_reference()

        # Cap the login window at the user's latest allowed login (if suspended/deleted)
        latest_allowed = context.latest_allowed_login()
        if latest_allowed is not None and latest_allowed < horizon:
            horizon = latest_allowed

        # If the capped horizon is before user creation, no login is possible
        if horizon <= context.user.created_at:
            return

        days_after_creation = self.random.randint(
            0,
            max(0, (horizon - context.user.created_at).days),
        )

        self.user_simulation.login_user(
            user=context.user,
            days_after_creation=days_after_creation,
            clock=self.clock.fork(start_at=context.user.created_at),
        )

        self.user_simulation.login_user(
            user=context.user,
            days_after_creation=days_after_creation,
            clock=self.clock.fork(start_at=context.user.created_at),
        )

    def generate_user_lifecycle(
        self,
        *,
        user_id: str | None = None,
    ):
        """Event-stream facade over generate_user_journey().

        Yields the user, then the subscriptions, then the payments; the account
        status and last_login are resolved inside generate_user_journey(), i.e.
        before the first event is yielded. Kept as a generator for consumers that
        want an event feed, and REQUIRED to go through generate_user_journey() so
        it can never diverge from the live orchestrator (19b).
        """
        user = self.generate_user(user_id=user_id)

        yield {
            "type": "user_created",
            "data": user,
        }

        context = self.generate_user_journey(
            user=user,
            subscription_count=self.random.randint(
                self.MIN_SUBSCRIPTIONS_PER_USER,
                self.MAX_SUBSCRIPTIONS_PER_USER,
            ),
        )

        for subscription in context.subscriptions:
            yield {
                "type": "subscription_created",
                "data": subscription,
            }

        for payment in context.payments:
            yield {
                "type": "payment_created",
                "data": payment,
            }

    def generate_users(
        self,
        *,
        count: int,
    ):
        for _ in range(count):
            yield self.generate_user()

    def resolve_billing_eligibility(
        self,
        *,
        users: List[UserEntity],
        subscriptions: List[SubscriptionEntity],
        payments: List[PaymentEntity],
        horizon: datetime | None = None,
    ) -> BillingEligibility:
        """R6 + R7 — apply the payment activation gate, then derive the eligibility map.

        R6: every subscription's persisted `is_active` is re-resolved from its OWN
        payment chain (`has_success_payment AND start <= horizon <= end`, free plans
        exempt), instead of trusting the temporal-only value the factory could
        compute before payments existed.

        R7: the returned map tells the caller which windows were actually PAID FOR
        and which accounts are STILL ACTIVE - i.e. the only (user, window) pairs the
        event stream is allowed to sample.

        The rules themselves live in
        `scripts.simulation.registry.billing_eligibility`, so the orchestrator only
        has to call this once and hand the map to the TemporalRegistry and to its
        candidate user pool. Deterministic: no RNG, no clock mutation.
        """
        billing_eligibility = BillingEligibility(
            users=users,
            subscriptions=subscriptions,
            payments=payments,
            horizon=horizon if horizon is not None else self._horizon_reference(),
        )

        billing_eligibility.apply_activation_gate()

        return billing_eligibility