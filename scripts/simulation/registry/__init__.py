"""
Simulation registries and derived eligibility maps.

`temporal_registry` / `match_registry` remain flat modules under
`scripts/simulation/`; this package holds the DERIVED maps that are computed from
generated data and then consumed by the orchestrator.
"""

from .billing_eligibility import (
    BillingCoverageReport,
    BillingEligibility,
    FREE_PLAN_TYPE,
    PREMIUM_PLAN_TYPE,
    SUCCESS_PAYMENT_STATUS,
    SubscriptionEligibility,
    is_within_horizon,
    partition_payments_by_subscription,
    subscription_has_success_payment,
)


__all__ = [
    "BillingCoverageReport",
    "BillingEligibility",
    "FREE_PLAN_TYPE",
    "PREMIUM_PLAN_TYPE",
    "SUCCESS_PAYMENT_STATUS",
    "SubscriptionEligibility",
    "is_within_horizon",
    "partition_payments_by_subscription",
    "subscription_has_success_payment",
]
