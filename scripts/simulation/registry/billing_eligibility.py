"""R6 / R7 — Phase 6/7: Payments as Activation Gate & Eligibility Map

Why this module exists
----------------------
R6 makes a SUCCESSFUL PAYMENT the condition for a subscription to be active:

    is_active == has_success_payment AND (start_date <= horizon <= end_date)

with free plans mathematically exempt (a free plan is never billed, so the gate
cannot apply to it). R7 then requires the event stream to only ever sample a
(user, timestamp) pair whose window was ACTUALLY PAID FOR and whose account is
STILL ACTIVE.

Both rules are pure set arithmetic over (subscriptions, payments, users), but
resolving them inline would force lookup maps and multi-branch loops into the
orchestrator (`run_simulation.py`) and into `video_simulation.py`. They live here
instead, so the orchestrator only has to resolve the map once and hand it over:

    eligibility = postgres_simulation.resolve_billing_eligibility(...)   # R6 + R7
    registry.load_from_memory(
        [s.to_dict() for s in subscriptions],
        videos,
        paid_subscription_ids=eligibility.paid_subscription_ids(),
        active_user_ids=eligibility.active_user_ids(),
    )

The module is deliberately stdlib-only and DB-free, so downstream consumers and
unit tests can reuse exactly the same rules.
See README.md#phase-markers-used-in-code-comments
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Sequence

from ..postgres.user_lifecycle import UserLifecycle


# Plan / payment vocabulary. Plain strings, because this module is a pure
# value-object layer over the Postgres simulation entities (which use str enums:
# plan_type_enum, payment_status_enum — see scripts/loading/load_postgres.py).
FREE_PLAN_TYPE = "free"
PREMIUM_PLAN_TYPE = "premium"
SUCCESS_PAYMENT_STATUS = "success"


# -- Phase 6: R6 — Pure Payment Rules --

def partition_payments_by_subscription(payments: Iterable) -> Dict[str, List]:
    """Index payments by `subscription_id` in ONE pass (O(P)).

    R6 is a per-subscription question ("did THIS window's billing succeed?"), so
    the naive implementation would scan the whole payment list once per
    subscription (O(S x P)). The index makes it O(S + P).
    """
    grouped: Dict[str, List] = {}
    for payment in payments:
        grouped.setdefault(payment.subscription_id, []).append(payment)
    return grouped


def subscription_has_success_payment(
    *,
    plan_type: str,
    payments_for_subscription: Sequence,
) -> bool:
    """R6 — did this subscription's billing ever succeed?

    Free plans are EXEMPT: they are never billed at all (the payment chain for a
    free plan is empty by construction — see PostgresSimulation), so "no success
    payment" is not a failure for them and must never deactivate the window.
    """
    if plan_type == FREE_PLAN_TYPE:
        return True

    return any(
        payment.payment_status == SUCCESS_PAYMENT_STATUS
        for payment in payments_for_subscription
    )


def is_within_horizon(
    *,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    horizon: Optional[datetime],
) -> bool:
    """R6 — `start_date <= horizon <= end_date` (horizon == SIMULATION_END).

    An unbounded horizon (None) cannot fail containment, so it returns True.
    """
    if horizon is None or start_date is None or end_date is None:
        return True

    return start_date <= horizon <= end_date


# -- Phase 6: R6 — Per-Subscription Verdict --

@dataclass(frozen=True, slots=True)
class SubscriptionEligibility:
    """R6 verdict for ONE subscription: the payment gate x the horizon."""
    subscription_id: str
    user_id: str
    plan_type: str
    has_success_payment: bool
    within_horizon: bool
    is_active: bool

    @property
    def is_paid_for(self) -> bool:
        """R7 coverage: this window was actually paid for (free plans exempt)."""
        return self.has_success_payment


@dataclass(frozen=True, slots=True)
class BillingCoverageReport:
    """Read-only telemetry for one run (nothing downstream reads it back)."""
    subscriptions_total: int
    subscriptions_paid_for: int
    subscriptions_unpaid: int
    subscriptions_active: int
    users_total: int
    users_active: int
    users_suspended: int
    users_deleted: int
    eligible_users: int
    eligible_windows: int


# -- Phase 6/7: R6 + R7 — The Derived Map --

class BillingEligibility:
    """Derived map: PAID-FOR windows x ACTIVE accounts.

    Constructed once per run from (users, subscriptions, payments) and then
    consumed by the `TemporalRegistry` eligibility gate and by step [6]'s
    candidate user pool. Deriving it draws NO random numbers and never touches
    the clock, so it is fully deterministic; the only mutation it performs is the
    explicit R6 write-back in `apply_activation_gate()`.
    """

    def __init__(
        self,
        *,
        users: Iterable,
        subscriptions: Iterable,
        payments: Iterable,
        horizon: Optional[datetime] = None,
        active_status: str = UserLifecycle.ACTIVE,
    ):
        self._horizon = horizon
        self._active_status = active_status

        # R6 telemetry, filled by apply_activation_gate(). Zero until then, so a
        # consumer that only derives the map (and never writes back) still reads a
        # meaningful value instead of None.
        self._deactivated_by_payment_gate = 0

        payments_by_subscription = partition_payments_by_subscription(payments)

        self._subscription_objects: Dict[str, Any] = {}
        self._verdicts: Dict[str, SubscriptionEligibility] = {}
        self._subscriptions_by_user: Dict[str, List[Any]] = {}

        for subscription in subscriptions:
            has_success_payment = subscription_has_success_payment(
                plan_type=subscription.plan_type,
                payments_for_subscription=payments_by_subscription.get(
                    subscription.subscription_id,
                    (),
                ),
            )
            within_horizon = is_within_horizon(
                start_date=subscription.start_date,
                end_date=subscription.end_date,
                horizon=horizon,
            )

            self._verdicts[subscription.subscription_id] = SubscriptionEligibility(
                subscription_id=subscription.subscription_id,
                user_id=subscription.user_id,
                plan_type=subscription.plan_type,
                has_success_payment=has_success_payment,
                within_horizon=within_horizon,
                is_active=bool(has_success_payment and within_horizon),
            )
            self._subscription_objects[subscription.subscription_id] = subscription
            self._subscriptions_by_user.setdefault(subscription.user_id, []).append(
                subscription
            )

        # R8 — account status was already resolved by the lifecycle evaluation
        # that runs inside PostgresSimulation.generate_user_journey(); this map
        # only READS it, it never recomputes it.
        self._user_order: tuple = tuple(user.user_id for user in users)
        self._account_status_by_user: Dict[str, str] = {
            user.user_id: user.account_status for user in users
        }
        self._active_user_ids: FrozenSet[str] = frozenset(
            user_id
            for user_id, status in self._account_status_by_user.items()
            if status == self._active_status
        )

        # R7 — the windows that were actually paid for, regardless of the owner's
        # account status (free plans are exempt, so they always count as paid).
        self._paid_subscription_ids: FrozenSet[str] = frozenset(
            verdict.subscription_id
            for verdict in self._verdicts.values()
            if verdict.is_paid_for
        )

        # R7 — the windows the event stream may use: paid for AND owned by an
        # account that survived the R8 lifecycle evaluation.
        self._eligible_subscription_ids: FrozenSet[str] = frozenset(
            verdict.subscription_id
            for verdict in self._verdicts.values()
            if verdict.is_paid_for and verdict.user_id in self._active_user_ids
        )
        self._eligible_user_ids: FrozenSet[str] = frozenset(
            verdict.user_id
            for verdict in self._verdicts.values()
            if verdict.subscription_id in self._eligible_subscription_ids
        )

    # -----------------------------------------------------------------
    # R6 — write-back
    # -----------------------------------------------------------------

    def apply_activation_gate(self) -> int:
        """R6 — write the payment-based verdict back onto every subscription.

        The SubscriptionFactory can only resolve the TEMPORAL half of `is_active`
        at creation time (payments do not exist yet), which is why a premium
        window whose billing failed used to be persisted as active. This resolves
        the other half, so `is_active` ends up equal to
        `has_success_payment AND within_horizon`, with free plans exempt.

        Returns the number of subscriptions the gate deactivated, so callers can
        report it without re-deriving anything.
        """
        deactivated = 0

        for subscription_id, verdict in self._verdicts.items():
            subscription = self._subscription_objects[subscription_id]

            if subscription.is_active and not verdict.is_active:
                deactivated += 1

            subscription.is_active = verdict.is_active

        self._deactivated_by_payment_gate = deactivated

        return deactivated

    @property
    def deactivated_by_payment_gate(self) -> int:
        """R6 telemetry: windows the payment gate flipped from active to inactive.

        These are the windows the temporal-only factory verdict would have
        persisted as active (a premium window whose billing never succeeded).
        Read by the run report; the gate itself is the only writer.
        """
        return self._deactivated_by_payment_gate

    # -----------------------------------------------------------------
    # R7 — accessors consumed by the orchestrator / TemporalRegistry
    # -----------------------------------------------------------------

    def paid_subscription_ids(self) -> FrozenSet[str]:
        """Subscription ids whose window was actually paid for (free exempt)."""
        return self._paid_subscription_ids

    def active_user_ids(self) -> FrozenSet[str]:
        """User ids whose account_status is still `active` (R8 verdict)."""
        return self._active_user_ids

    def eligible_subscription_ids(self) -> FrozenSet[str]:
        """Paid-for windows owned by an active account: R7's allowed set."""
        return self._eligible_subscription_ids

    def event_eligible_user_ids(self) -> List[str]:
        """Step [6] candidate pool: ACTIVE accounts holding >= 1 paid window.

        A user without a paid window can never be placed (the registry gate would
        reject every probe), so including them would only burn the emitter's fixed
        probe budget. Returned in the original `users` order so uniform sampling
        stays reproducible for a given seed.
        """
        return [
            user_id
            for user_id in self._user_order
            if user_id in self._eligible_user_ids
        ]

    def eligible_subscriptions_by_user(self) -> Dict[str, List[Any]]:
        """`{user_id: [subscription, ...]}` restricted to R7-eligible windows.

        Shape-compatible with `build_user_intervals()` and the R2 non-overlap
        checker, which both read `.start_date` / `.end_date` off the objects, so
        the eligible-audience bound (R13) is computed over exactly the population
        the emitter is allowed to serve.
        """
        return {
            user_id: [
                subscription
                for subscription in user_subscriptions
                if subscription.subscription_id in self._eligible_subscription_ids
            ]
            for user_id, user_subscriptions in self._subscriptions_by_user.items()
            if user_id in self._active_user_ids
        }

    # -- Phase 6/7: Introspection --

    def user_is_active(self, user_id: str) -> bool:
        return user_id in self._active_user_ids

    def subscription_eligibility(
        self,
        subscription_id: str,
    ) -> Optional[SubscriptionEligibility]:
        return self._verdicts.get(subscription_id)

    def is_eligible_pair(self, user_id: str, subscription_id: str) -> bool:
        """R7 membership test for one (user, window) pair."""
        return (
            user_id in self._active_user_ids
            and subscription_id in self._eligible_subscription_ids
        )

    def coverage_report(self) -> BillingCoverageReport:
        """Eligibility telemetry for the run's console report."""
        verdicts = list(self._verdicts.values())

        return BillingCoverageReport(
            subscriptions_total=len(verdicts),
            subscriptions_paid_for=sum(1 for v in verdicts if v.is_paid_for),
            subscriptions_unpaid=sum(1 for v in verdicts if not v.is_paid_for),
            subscriptions_active=sum(1 for v in verdicts if v.is_active),
            users_total=len(self._user_order),
            users_active=len(self._active_user_ids),
            users_suspended=self._count_status(UserLifecycle.SUSPENDED),
            users_deleted=self._count_status(UserLifecycle.DELETED),
            eligible_users=len(self._eligible_user_ids),
            eligible_windows=len(self._eligible_subscription_ids),
        )

    def _count_status(self, status: str) -> int:
        return sum(
            1
            for current in self._account_status_by_user.values()
            if current == status
        )

