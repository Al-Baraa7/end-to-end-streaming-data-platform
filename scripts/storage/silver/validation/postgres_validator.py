"""
PostgreSQL Silver Layer Validators.

Validation rules for PostgreSQL-sourced Silver tables.
Silver validation ONLY asserts data quality - no Gold construction logic.
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


# Valid country codes (ISO 3166-1 alpha-2)
VALID_COUNTRIES = frozenset([
    "US", "CA", "GB", "DE", "FR", "AU", "JP", "BR", "IN", "MX",
    "ES", "IT", "NL", "PL", "BE", "SE", "NO", "DK", "FI", "CH",
    "AT", "IE", "PT", "GR", "CZ", "HU", "RO", "BG", "HR", "SK",
    "LT", "LV", "EE", "SI", "MT", "CY", "LU", "IS", "LI", "MC",
    "SM", "VA", "AD", "NZ", "SG", "HK", "KR", "TW", "IL", "AE",
    "SA", "QA", "KW", "BH", "OM", "JO", "LB", "EG", "MA", "ZA",
    "NG", "KE", "GH", "TZ", "UG", "RW", "ET", "CM", "CI", "SN"
])

VALID_CURRENCIES = frozenset(["USD", "EUR", "GBP", "JPY", "CAD", "AUD"])
VALID_PAYMENT_STATUSES = frozenset(["succeeded", "pending", "failed", "refunded", "canceled"])
def validate_users_silver(df: DataFrame) -> ValidationSuite:
    """
    Validate Silver users table.
    
    Checks:
    - PK uniqueness on user_id
    - Required fields not null
    - Country values in valid set (if present)
    - Data quality flag consistency
    - Schema conformance
    - Date bounds reasonableness
    """
    suite = ValidationSuite()
    
    # Primary key uniqueness
    suite.add(assert_pk_unique(df, ["user_id"], "users_pk_unique"))
    
    # Required non-null fields
    suite.add(assert_no_nulls(df, ["user_id", "created_at"], "users_required_not_null"))
    
    # Country validation (if present)
    suite.add(assert_column_values_in_set(df, "country", VALID_COUNTRIES, "users_country_valid", allow_null=True))
    
    # DQ flag consistency
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
    
    # Schema conformance
    suite.add(assert_schema_conformance(df, USERS_SILVER_SCHEMA, "users_schema", strict=True))
    
    # Date bounds
    suite.add(assert_date_bounds(df, "created_at", min_date="2020-01-01", check_name="users_created_at_bounds"))
    suite.add(assert_date_bounds(df, "last_login", min_date="2020-01-01", check_name="users_last_login_bounds"))
    
    return suite


def validate_subscriptions_silver(df: DataFrame) -> ValidationSuite:
    """
    Validate Silver subscriptions table.
    """
    suite = ValidationSuite()
    
    suite.add(assert_pk_unique(df, ["subscription_id"], "subscriptions_pk_unique"))
    suite.add(assert_no_nulls(df, ["subscription_id", "user_id", "plan_type", "start_date"], "subscriptions_required_not_null"))
    suite.add(assert_flag_consistency(
        df, "is_end_date_before_start_date", "end_date",
        "end_date IS NOT NULL AND start_date IS NOT NULL AND end_date < start_date", "subscriptions_dq_end_before_start"
    ))
    suite.add(assert_schema_conformance(df, SUBSCRIPTIONS_SILVER_SCHEMA, "subscriptions_schema", strict=True))
    suite.add(assert_date_bounds(df, "start_date", min_date="2020-01-01", check_name="subscriptions_start_date_bounds"))
    suite.add(assert_date_bounds(df, "end_date", min_date="2020-01-01", check_name="subscriptions_end_date_bounds"))
    
    return suite


def validate_payments_silver(df: DataFrame) -> ValidationSuite:
    """
    Validate Silver payments table.
    
    NOTE: 'status' and 'billing_reason' are NON-EXISTENT in source schema.
    They are completely excluded from Silver - schema conformance check ensures this.
    """
    suite = ValidationSuite()
    
    suite.add(assert_pk_unique(df, ["payment_id"], "payments_pk_unique"))
    suite.add(assert_no_nulls(df, ["payment_id", "subscription_id", "user_id", "amount", "currency", "payment_date", "payment_status"], "payments_required_not_null"))
    suite.add(assert_column_values_in_set(df, "currency", VALID_CURRENCIES, "payments_currency_valid", allow_null=False))
    suite.add(assert_column_values_in_set(df, "payment_status", VALID_PAYMENT_STATUSES, "payments_status_valid", allow_null=False))
    suite.add(assert_flag_consistency(
        df, "amount", "amount",
        "amount > 0", "payments_amount_positive"
    ))
    suite.add(assert_schema_conformance(df, PAYMENTS_SILVER_SCHEMA, "payments_schema", strict=True))
    suite.add(assert_date_bounds(df, "payment_date", min_date="2020-01-01", check_name="payments_payment_date_bounds"))
    
    return suite