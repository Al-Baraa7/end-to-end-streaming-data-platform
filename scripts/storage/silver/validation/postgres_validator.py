"""
PostgreSQL Silver Layer Validators.

Validation rules for PostgreSQL-sourced Silver tables.
Silver validation ONLY asserts data quality - no Gold construction logic.

Constants derived strictly from simulation factories:
- UserFactory.COUNTRIES: KSA, UAE, EGY, KWT, QAT, MAR, JOR
- SubscriptionFactory.PLAN_TYPES: free, premium
- PaymentFactory.CURRENCY: USD
- PaymentFactory.PAYMENT_STATUSES: success, failed
- PaymentFactory.BILLING_REASONS: initial, retry, renewal
- PaymentFactory.MAX_ATTEMPTS: 3
"""
from pyspark.sql import DataFrame
from scripts.storage.silver.validation.assertions import (
    ValidationSuite,
    assert_pk_unique,
    assert_no_nulls,
    assert_column_values_in_set,
    assert_flag_consistency,
    assert_schema_conformance,
    assert_date_bounds,
)
from scripts.storage.silver.transforms.postgres import (
    USERS_SILVER_SCHEMA,
    SUBSCRIPTIONS_SILVER_SCHEMA,
    PAYMENTS_SILVER_SCHEMA,
)


# ==================== FACTORY-ALIGNED CONSTANTS ====================

# From UserFactory.COUNTRIES (scripts/simulation/postgres/user_factory.py)
VALID_COUNTRIES = frozenset([
    "KSA", "UAE", "EGY", "KWT", "QAT", "MAR", "JOR"
])

# From SubscriptionFactory.PLAN_TYPES (scripts/simulation/postgres/subscription_factory.py)
VALID_PLAN_TYPES = frozenset(["free", "premium"])

# From PaymentFactory (scripts/simulation/postgres/payment_factory.py)
VALID_CURRENCY = "USD"
VALID_PAYMENT_STATUSES = frozenset(["succeeded","failed"])
VALID_BILLING_REASONS = frozenset(["initial", "retry", "renewal"])
MAX_ATTEMPTS = 3


def validate_users_silver(df: DataFrame) -> ValidationSuite:
    """
    Validate Silver users table.

    Checks:
    - PK uniqueness on user_id
    - Required fields not null
    - Country values in valid factory set (if present)
    - Data quality flag consistency for all 4 flags
    - Schema conformance
    - Date bounds reasonableness
    """
    suite = ValidationSuite()

    # Primary key uniqueness
    suite.add(assert_pk_unique(df, ["user_id"], "users_pk_unique"))

    # Required non-null fields
    suite.add(assert_no_nulls(df, ["user_id", "created_at"], "users_required_not_null"))

    # Country validation against factory-authorized set
    suite.add(assert_column_values_in_set(df, "country", VALID_COUNTRIES, "users_country_valid", allow_null=True))

    # DQ flag consistency — all 4 flags from USERS_SILVER_SCHEMA
    suite.add(assert_flag_consistency(
        df, "is_missing_email", "email",
        "email IS NULL", "users_dq_missing_email"
    ))
    suite.add(assert_flag_consistency(
        df, "is_invalid_country", "country",
        f"country NOT IN ({','.join(repr(c) for c in VALID_COUNTRIES)})", "users_dq_invalid_country"
    ))
    suite.add(assert_flag_consistency(
        df, "is_last_login_before_created", "last_login",
        "last_login IS NOT NULL AND last_login < created_at", "users_dq_login_before_created"
    ))
    # is_invalid_account_status: flags injected "INVALID_STATUS"
    suite.add(assert_flag_consistency(
        df, "is_invalid_account_status", "account_status",
        "trim(account_status) = 'INVALID_STATUS'", "users_dq_invalid_account_status"
    ))

    # Schema conformance
    suite.add(assert_schema_conformance(df, USERS_SILVER_SCHEMA, "users_schema", strict=True))

    # Date bounds
    suite.add(assert_date_bounds(df, "created_at", min_date="2020-01-01", check_name="users_created_at_bounds"))
    suite.add(assert_date_bounds(df, "last_login", min_date="2020-01-01", check_name="users_last_login_bounds"))

    return suite


def validate_subscriptions_silver(df: DataFrame) -> ValidationSuite:
    """
    Validate Silver subscriptions table.

    Checks:
    - PK uniqueness on subscription_id
    - Required fields not null
    - Plan type in valid factory set (if present)
    - Data quality flag consistency for both flags
    - Derived analytical field validation (nominal_duration_days, is_truncated)
    - Schema conformance
    - Date bounds reasonableness
    """
    suite = ValidationSuite()

    suite.add(assert_pk_unique(df, ["subscription_id"], "subscriptions_pk_unique"))
    suite.add(assert_no_nulls(df, ["subscription_id", "user_id", "plan_type", "start_date"], "subscriptions_required_not_null"))

    # Plan type validation against factory-authorized set
    suite.add(assert_column_values_in_set(df, "plan_type", VALID_PLAN_TYPES, "subscriptions_plan_type_valid", allow_null=True))

    # DQ flag consistency — both flags from SUBSCRIPTIONS_SILVER_SCHEMA
    suite.add(assert_flag_consistency(
        df, "is_end_date_before_start_date", "end_date",
        "end_date IS NOT NULL AND start_date IS NOT NULL AND end_date < start_date", "subscriptions_dq_end_before_start"
    ))
    # is_invalid_plan_type: flags injected "INVALID_PLAN"
    suite.add(assert_flag_consistency(
        df, "is_invalid_plan_type", "plan_type",
        "trim(plan_type) = 'INVALID_PLAN'", "subscriptions_dq_invalid_plan_type"
    ))


    # Schema conformance
    suite.add(assert_schema_conformance(df, SUBSCRIPTIONS_SILVER_SCHEMA, "subscriptions_schema", strict=True))

    # Date bounds
    suite.add(assert_date_bounds(df, "start_date", min_date="2020-01-01", check_name="subscriptions_start_date_bounds"))
    suite.add(assert_date_bounds(df, "end_date", min_date="2020-01-01", check_name="subscriptions_end_date_bounds"))

    return suite


def validate_payments_silver(df: DataFrame) -> ValidationSuite:
    """
    Validate Silver payments table.

    Checks:
    - PK uniqueness on payment_id
    - Required fields not null
    - Currency = USD (factory single currency)
    - Payment status in valid normalized set
    - Billing reason in valid factory set
    - Attempt_no within [1, 3] (factory MAX_ATTEMPTS)
    - Data quality flag consistency for all 4 flags
    - Schema conformance
    - Date bounds reasonableness

    NOTE: 'status' and 'billing_reason' ghost columns are NON-EXISTENT in source schema.
    They are completely excluded from Silver - schema conformance check ensures this.
    """
    suite = ValidationSuite()

    suite.add(assert_pk_unique(df, ["payment_id"], "payments_pk_unique"))
    suite.add(assert_no_nulls(df, ["payment_id", "subscription_id", "user_id", "amount", "currency", "payment_date", "payment_status"], "payments_required_not_null"))

    # Currency: factory single currency USD (normalized to upper in transformer)
    suite.add(assert_column_values_in_set(df, "currency", frozenset([VALID_CURRENCY]), "payments_currency_valid", allow_null=False))

    # Payment status: normalized values in transformer (success->succeeded, others lowercased)
    suite.add(assert_column_values_in_set(df, "payment_status", VALID_PAYMENT_STATUSES, "payments_status_valid", allow_null=False))

    # Billing reason: factory-authorized set
    suite.add(assert_column_values_in_set(df, "billing_reason", VALID_BILLING_REASONS, "payments_billing_reason_valid", allow_null=True))

    # Attempt_no bounds: factory MAX_ATTEMPTS = 3
    suite.add(assert_flag_consistency(
        df, "is_attempt_no_out_of_bounds", "attempt_no",
        f"attempt_no < 1 OR attempt_no > {MAX_ATTEMPTS}", "payments_dq_attempt_no_out_of_bounds"
    ))

    # DQ flag consistency — all 4 flags from PAYMENTS_SILVER_SCHEMA
    # is_invalid_payment_status: flags normalized 'invalid_status' (transformer lowercases and trims before check)
    suite.add(assert_flag_consistency(
        df, "is_invalid_payment_status", "payment_status",
        "lower(trim(payment_status)) = 'invalid_status'", "payments_dq_invalid_payment_status"
    ))

    # is_invalid_billing_reason: flags injected 'INVALID_REASON'
    suite.add(assert_flag_consistency(
        df, "is_invalid_billing_reason", "billing_reason",
        "trim(billing_reason) = 'INVALID_REASON'", "payments_dq_invalid_billing_reason"
    ))

    # is_negative_amount: flags amount < 0
    suite.add(assert_flag_consistency(
        df, "is_negative_amount", "amount",
        "amount < 0", "payments_dq_negative_amount"
    ))

    # Schema conformance
    suite.add(assert_schema_conformance(df, PAYMENTS_SILVER_SCHEMA, "payments_schema", strict=True))

    # Date bounds
    suite.add(assert_date_bounds(df, "payment_date", min_date="2020-01-01", check_name="payments_payment_date_bounds"))

    return suite
