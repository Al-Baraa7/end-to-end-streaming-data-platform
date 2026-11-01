"""
PostgreSQL Silver Layer Transformations.

Pure, deterministic transformation functions for PostgreSQL-sourced data.
Each function takes a Bronze DataFrame and returns a Silver DataFrame with
explicit schema contracts. No I/O, no side effects, fully testable.
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, DateType, DecimalType, IntegerType
)


# ==================== SILVER SCHEMA CONTRACTS ====================

USERS_SILVER_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("last_login", TimestampType(), True),
    StructField("is_missing_email", BooleanType(), False),
    StructField("is_invalid_country", BooleanType(), False),
    StructField("is_last_login_before_created", BooleanType(), False),
    StructField("created_date", DateType(), False),
    StructField("last_login_date", DateType(), True),
    # R15: Account status for lifecycle tracking
    StructField("account_status", StringType(), True),
])

SUBSCRIPTIONS_SILVER_SCHEMA = StructType([
    StructField("subscription_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("plan_type", StringType(), False),
    StructField("start_date", TimestampType(), False),
    StructField("end_date", TimestampType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("auto_renew", BooleanType(), False),
    StructField("is_end_date_before_start_date", BooleanType(), False),
    StructField("start_date_day", DateType(), False),
    StructField("end_date_day", DateType(), True),
    # R15: Nominal duration for subscription analytics
    StructField("nominal_duration_days", IntegerType(), True),
    # R15: Truncation flag for incomplete subscriptions
    StructField("is_truncated", BooleanType(), False),
])

PAYMENTS_SILVER_SCHEMA = StructType([
    StructField("payment_id", StringType(), False),
    StructField("subscription_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DecimalType(10, 2), False),
    StructField("currency", StringType(), False),
    StructField("payment_date", TimestampType(), False),
    StructField("payment_status", StringType(), False),
    StructField("payment_date_day", DateType(), False),
    # R15: Attempt number for retry tracking
    StructField("attempt_no", IntegerType(), False),
    # R15: Billing reason for payment categorization
    StructField("billing_reason", StringType(), True),
])
# ==================== TRANSFORMATION FUNCTIONS ====================

def transform_users(df_bronze: DataFrame) -> DataFrame:
    """
    Transform Bronze users to Silver.
    
    Business Rules:
    - Preserve user_id unchanged (PK)
    - Trim whitespace from email
    - Preserve NULL email values (flag them)
    - Normalize country: trim + uppercase only (preserve invalid values, flag them)
    - last_login is now clean at source - read directly as engagement metric
    - Add validation flags for data quality issues
    - Add date-level columns for analytical partitioning
    
    Args:
        df_bronze: Bronze users DataFrame with raw columns
        
    Returns:
        Silver users DataFrame conforming to USERS_SILVER_SCHEMA
    """
    return (
        df_bronze
        # Technical cleaning
        .withColumn("email", F.trim(F.col("email")))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        
        # Data quality flags (preserve intentional dirty data, flag it)
        .withColumn(
            "is_missing_email",
            F.col("email").isNull()
        )
        .withColumn(
            "is_invalid_country",
            ~F.col("country").isin([
                "US", "CA", "GB", "DE", "FR", "AU", "JP", "BR", "IN", "MX",
                "ES", "IT", "NL", "PL", "BE", "SE", "NO", "DK", "FI", "CH",
                "AT", "IE", "PT", "GR", "CZ", "HU", "RO", "BG", "HR", "SK",
                "LT", "LV", "EE", "SI", "MT", "CY", "LU", "IS", "LI", "MC",
                "SM", "VA", "AD", "NZ", "SG", "HK", "KR", "TW", "IL", "AE",
                "SA", "QA", "KW", "BH", "OM", "JO", "LB", "EG", "MA", "ZA",
                "NG", "KE", "GH", "TZ", "UG", "RW", "ET", "CM", "CI", "SN"
            ])
        )
        .withColumn(
            "is_last_login_before_created",
            F.col("last_login").isNotNull() &
            (F.col("last_login") < F.col("created_at"))
        )
        
        # Analytical date columns
        .withColumn("created_date", F.to_date("created_at"))
        .withColumn("last_login_date", F.to_date("last_login"))
        .withColumn("account_status", F.col("account_status"))

        
        # Enforce explicit schema contract - select only defined columns
        .select(*[f.name for f in USERS_SILVER_SCHEMA.fields])
    )


def transform_subscriptions(df_bronze: DataFrame) -> DataFrame:
    """
    Transform Bronze subscriptions to Silver.
    
    Business Rules:
    - Preserve all identifiers and relationships unchanged
    - Preserve timestamps as-is (no silent correction)
    - auto_renew is fully enforced at source - read directly
    - Add validation flag for end_date before start_date
    - Add date-level columns for analytical use
    
    Args:
        df_bronze: Bronze subscriptions DataFrame
        
    Returns:
        Silver subscriptions DataFrame conforming to SUBSCRIPTIONS_SILVER_SCHEMA
    """
    return (
        df_bronze
        # Data quality flag - preserve invalid chronology, flag it
        .withColumn(
            "is_end_date_before_start_date",
            F.col("end_date").isNotNull() &
            (F.col("end_date") < F.col("start_date"))
        )
        
        # Analytical date columns
        .withColumn("start_date_day", F.to_date("start_date"))
        .withColumn("end_date_day", F.to_date("end_date"))
        .withColumn("nominal_duration_days", F.col("nominal_duration_days").cast("int")) \
        .withColumn("is_truncated", F.col("is_truncated").cast("boolean"))

        
        # Enforce explicit schema contract
        .select(*[f.name for f in SUBSCRIPTIONS_SILVER_SCHEMA.fields])
    )


def transform_payments(df_bronze: DataFrame) -> DataFrame:
    """
    Transform Bronze payments to Silver.
    
    Business Rules:
    - Preserve identifiers and relationships unchanged
    - Normalize amount precision from decimal(38,18) to decimal(10,2)
    - Normalize currency and payment_status: trim + upper
    - Preserve payment timestamps
    - Add date-level payment column
    - NOTE: 'status' and 'billing_reason' columns are NON-EXISTENT in source schema.
      Do not reference, drop, or handle them - they are not in the cosmos.
    
    Args:
        df_bronze: Bronze payments DataFrame
        
    Returns:
        Silver payments DataFrame conforming to PAYMENTS_SILVER_SCHEMA
    """
    return (
        df_bronze
        # Normalize amount precision
        .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
        
        # Normalize string fields
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn(
            "payment_status",
            F.when(F.lower(F.trim(F.col("payment_status"))) == "success", "succeeded")
            .otherwise(F.lower(F.trim(F.col("payment_status"))))
        )
        
        # Analytical date column
        .withColumn("payment_date_day", F.to_date("payment_date"))
        .withColumn("attempt_no", F.col("attempt_no").cast("int")) \
        .withColumn("billing_reason", F.col("billing_reason"))

        
        # Enforce explicit schema contract - ghost columns excluded by selection
        .select(*[f.name for f in PAYMENTS_SILVER_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def transform_all_postgres(
    df_users: DataFrame,
    df_subscriptions: DataFrame,
    df_payments: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Transform all PostgreSQL Bronze tables to Silver.
    
    Convenience function for jobs that process all tables together.
    
    Returns:
        Tuple of (users_silver, subscriptions_silver, payments_silver)
    """
    return (
        transform_users(df_users),
        transform_subscriptions(df_subscriptions),
        transform_payments(df_payments),
    )