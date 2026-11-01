# import psycopg2
# from psycopg2.extras import execute_values
# from scripts.generation.user_and_subscriptions_pg import generate_user_ids, generate_users, generate_subscriptions, generate_payments
# from config import DB_HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT



# user_ids = generate_user_ids()
# users = generate_users(user_ids)
# subs = generate_subscriptions(user_ids)
# payments = generate_payments(subs)

# conn = psycopg2.connect(
#     host=DB_HOST,
#     database=DB_NAME,
#     user=DB_USER,
#     password=DB_PASSWORD,
#     port=DB_PORT
# )
# cur = conn.cursor()

# def create_tables(cur):
#     cur.execute("""
#     DO $$ BEGIN
#         CREATE TYPE account_status_enum AS ENUM ('active', 'suspended', 'deleted');
#         CREATE TYPE plan_type_enum AS ENUM ('free', 'premium');
#         CREATE TYPE payment_status_enum AS ENUM ('success', 'failed', 'refunded');
#     EXCEPTION
#         WHEN duplicate_object THEN NULL;
#     END $$;
#     """)

#     cur.execute("""
#     CREATE TABLE IF NOT EXISTS users (
#         user_id UUID PRIMARY KEY,
#         email TEXT,
#         country TEXT,
#         account_status account_status_enum,
#         created_at TIMESTAMP,
#         last_login TIMESTAMP
#     );
#     """)

#     cur.execute("""
#     CREATE TABLE IF NOT EXISTS subscription (
#         subscription_id UUID PRIMARY KEY,
#         user_id UUID REFERENCES users(user_id),
#         plan_type plan_type_enum,
#         start_date TIMESTAMP,
#         end_date TIMESTAMP,
#         is_active BOOLEAN,
#         auto_renew BOOLEAN
#     );
#     """)

#     cur.execute("""
#     CREATE TABLE IF NOT EXISTS payments (
#         payment_id UUID PRIMARY KEY,
#         subscription_id UUID REFERENCES subscription(subscription_id),
#         amount NUMERIC,
#         currency TEXT,
#         payment_date TIMESTAMP,
#         payment_status payment_status_enum
#     );
#     """)

# def insert_users(cur, users):
#     users_values = [
#         (u['user_id'], u['email'], u['country'],
#          u['account_status'], u['created_at'], u['last_login'])
#         for u in users
#     ]
#     execute_values(cur, """
#         INSERT INTO users (user_id, email, country, account_status, created_at, last_login)
#         VALUES %s
#     """, users_values)

# def insert_subscriptions(cur, subs):
#     subs_values = [
#         (s['subscription_id'], s['user_id'], s['plan_type'],
#          s['start_date'], s['end_date'], s['is_active'], s['auto_renew'])
#         for s in subs
#     ]
#     execute_values(cur, """
#         INSERT INTO subscription (subscription_id, user_id, plan_type,
#         start_date, end_date, is_active, auto_renew)
#         VALUES %s
#     """, subs_values)

# def insert_payments(cur, payments):
#     payments_values = [
#         (p['payment_id'], p['subscription_id'], p['amount'],
#          p['currency'], p['payment_date'], p['payment_status'])
#         for p in payments
#     ]
#     execute_values(cur, """
#         INSERT INTO payments (payment_id, subscription_id, amount,
#         currency, payment_date, payment_status)
#         VALUES %s
#     """, payments_values)

# try:
#     create_tables(cur)
#     insert_users(cur, users)
#     insert_subscriptions(cur, subs)
#     insert_payments(cur, payments)
#     conn.commit()
# except Exception as e:
#     conn.rollback()
#     print(f"Error: {e}")
# finally:
#     cur.close()
#     conn.close()

import psycopg2
from psycopg2.extras import execute_values

from config import (
    DB_HOST,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_PORT,
)


BATCH_SIZE = 1000


def create_tables(cur):
    # Drop old enum types if they exist (clean up from previous runs)
    cur.execute("""
        DROP TYPE IF EXISTS account_status_enum CASCADE;
        DROP TYPE IF EXISTS plan_type_enum CASCADE;
        DROP TYPE IF EXISTS payment_status_enum CASCADE;
        DROP TYPE IF EXISTS billing_reason_enum CASCADE;
        DROP TYPE IF EXISTS content_type_enum CASCADE;
        DROP TYPE IF EXISTS payment_method_enum CASCADE;
        DROP TYPE IF EXISTS video_status_enum CASCADE;
        DROP TYPE IF EXISTS currency_enum CASCADE;
        DROP TYPE IF EXISTS country_enum CASCADE;
        DROP TYPE IF EXISTS device_type_enum CASCADE;
        DROP TYPE IF EXISTS interaction_type_enum CASCADE;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id UUID PRIMARY KEY,
            email TEXT,
            country VARCHAR(10),
            account_status VARCHAR(50),
            created_at TIMESTAMP,
            last_login TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS subscription (
            subscription_id UUID PRIMARY KEY,
            user_id UUID REFERENCES users(user_id),
            plan_type VARCHAR(50),
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            is_active BOOLEAN,
            auto_renew BOOLEAN
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS plan_pricing (
            plan_type VARCHAR(50) NOT NULL,
            duration_days INT NOT NULL,
            amount NUMERIC NOT NULL,
            currency VARCHAR(10) NOT NULL DEFAULT 'USD',
            PRIMARY KEY (plan_type, duration_days)
        );
    """)

    # Phase 5 - R15 Gold DDL Change: Seed the single plan/pricing table as the only source of amounts
    cur.execute("""
        INSERT INTO plan_pricing (plan_type, duration_days, amount, currency)
        VALUES
            ('free', 30, 0.00, 'USD'),
            ('free', 90, 0.00, 'USD'),
            ('premium', 30, 9.99, 'USD'),
            ('premium', 90, 14.99, 'USD'),
            ('premium', 365, 29.99, 'USD')
        ON CONFLICT (plan_type, duration_days) DO NOTHING;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            payment_id UUID PRIMARY KEY,
            subscription_id UUID
                REFERENCES subscription(subscription_id),
            user_id UUID
                REFERENCES users(user_id),
            amount NUMERIC,
            currency VARCHAR(10),
            payment_date TIMESTAMP,
            payment_status VARCHAR(50),
            -- Phase 5 - R15 Gold DDL Change: attempt_no + billing_reason on PaymentEntity
            attempt_no INT NOT NULL DEFAULT 1,
            billing_reason VARCHAR(50) NOT NULL DEFAULT 'initial'
        );
    """)


def insert_users(cur, users):
    values = [
        (
            user.user_id,
            user.email,
            user.country,
            user.account_status,
            user.created_at,
            user.last_login,
        )
        for user in users
    ]

    if not values:
        return

    execute_values(
        cur,
        """
        INSERT INTO users (
            user_id,
            email,
            country,
            account_status,
            created_at,
            last_login
        )
        VALUES %s
        """,
        values,
    )


def insert_subscriptions(cur, subscriptions):
    values = [
        (
            subscription.subscription_id,
            subscription.user_id,
            subscription.plan_type,
            subscription.start_date,
            subscription.end_date,
            subscription.is_active,
            subscription.auto_renew,
        )
        for subscription in subscriptions
    ]

    if not values:
        return

    execute_values(
        cur,
        """
        INSERT INTO subscription (
            subscription_id,
            user_id,
            plan_type,
            start_date,
            end_date,
            is_active,
            auto_renew
        )
        VALUES %s
        """,
        values,
    )


def insert_payments(cur, payments):
    values = [
        (
            payment.payment_id,
            payment.subscription_id,
            payment.user_id,
            payment.amount,
            payment.currency,
            payment.payment_date,
            payment.payment_status,
            payment.attempt_no,
            payment.billing_reason,
        )
        for payment in payments
    ]

    if not values:
        return

    execute_values(
        cur,
        """
        INSERT INTO payments (
            payment_id,
            subscription_id,
            user_id,
            amount,
            currency,
            payment_date,
            payment_status,
            attempt_no,
            billing_reason
        )
        VALUES %s
        """,
        values,
    )


def load_postgres(
    *,
    users,
    subscriptions,
    payments,
):
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )

    try:
        with conn.cursor() as cur:
            create_tables(cur)

            insert_users(cur, users)

            insert_subscriptions(
                cur,
                subscriptions,
            )

            insert_payments(
                cur,
                payments,
            )

        conn.commit()

        print("PostgreSQL load completed")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()
        print("PostgreSQL connection closed")