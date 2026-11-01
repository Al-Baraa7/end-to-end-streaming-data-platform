"""
Test Pristine Objects Unchanged - R19
"""

import pytest
from datetime import datetime, timezone
from copy import deepcopy

from scripts.simulation.anomaly_coordinator import create_coordinator
from scripts.simulation.postgres.user_entity import UserEntity
from scripts.simulation.postgres.subscription_entity import SubscriptionEntity
from scripts.simulation.postgres.payment_entity import PaymentEntity
from scripts.simulation.dirty_data.postgres_dirty_injection import PostgresDirtyDataInjector


class TestPristineObjectsUnchanged:
    """Verify pristine objects remain unchanged after dirty injection."""

    def test_pristine_users_unchanged_after_injection(self):
        """Pristine user objects remain unchanged after dirty injection."""
        # Use coordinator for shared RandomProvider
        coordinator = create_coordinator(
            seed=42,
            user_count=100,
            subscription_count=100,
            payment_count=100,
            video_count=50,
            event_count=500,
        )
        pg_rates = coordinator.get_injector_kwargs()["postgres"]
        injector = PostgresDirtyDataInjector(random_provider=coordinator.random_provider)
        
        original_users = [UserEntity(
            user_id=f"user_{i}", email=f"u{i}@e.com", country="KSA",
            account_status="active", created_at=datetime(2026,1,1,tzinfo=timezone.utc),
            last_login=datetime(2026,6,1,tzinfo=timezone.utc)) for i in range(100)]
        
        pristine_copies = [deepcopy(u) for u in original_users]
        
        list(injector.inject_users(users=original_users, anomaly_rate=pg_rates["user_anomaly_rate"]))
        
        for orig, pristine in zip(original_users, pristine_copies):
            assert orig.email == pristine.email
            assert orig.country == pristine.country
            assert orig.account_status == pristine.account_status
            assert orig.created_at == pristine.created_at
            assert orig.last_login == pristine.last_login

    def test_pristine_subscriptions_unchanged_after_injection(self):
        """Pristine subscription objects remain unchanged after dirty injection."""
        # Use coordinator for shared RandomProvider
        coordinator = create_coordinator(
            seed=42,
            user_count=100,
            subscription_count=100,
            payment_count=100,
            video_count=50,
            event_count=500,
        )
        pg_rates = coordinator.get_injector_kwargs()["postgres"]
        injector = PostgresDirtyDataInjector(random_provider=coordinator.random_provider)
        
        original_subs = [SubscriptionEntity(
            subscription_id=f"sub_{i}", user_id=f"user_{i}", plan_type="premium",
            start_date=datetime(2026,1,1,tzinfo=timezone.utc),
            end_date=datetime(2026,12,31,tzinfo=timezone.utc), is_active=True, auto_renew=True) for i in range(100)]
        
        pristine_copies = [deepcopy(s) for s in original_subs]
        list(injector.inject_subscriptions(subscriptions=original_subs, anomaly_rate=pg_rates["subscription_anomaly_rate"]))
        
        for orig, pristine in zip(original_subs, pristine_copies):
            assert orig.plan_type == pristine.plan_type
            assert orig.start_date == pristine.start_date
            assert orig.end_date == pristine.end_date
            assert orig.is_active == pristine.is_active
            assert orig.user_id == pristine.user_id

    def test_pristine_payments_unchanged_after_injection(self):
        """Pristine payment objects remain unchanged after dirty injection."""
        # Use coordinator for shared RandomProvider
        coordinator = create_coordinator(
            seed=42,
            user_count=100,
            subscription_count=100,
            payment_count=100,
            video_count=50,
            event_count=500,
        )
        pg_rates = coordinator.get_injector_kwargs()["postgres"]
        injector = PostgresDirtyDataInjector(random_provider=coordinator.random_provider)
        
        original_pays = [PaymentEntity(
            payment_id=f"pay_{i}", subscription_id=f"sub_{i}", user_id=f"user_{i}",
            amount=9.99, currency="USD", payment_date=datetime(2026,6,1,tzinfo=timezone.utc),
            payment_status="success", attempt_no=1, billing_reason="initial") for i in range(100)]
        
        pristine_copies = [deepcopy(p) for p in original_pays]
        list(injector.inject_payments(payments=original_pays, anomaly_rate=pg_rates["payment_anomaly_rate"]))
        
        for orig, pristine in zip(original_pays, pristine_copies):
            assert orig.payment_status == pristine.payment_status
            assert orig.billing_reason == pristine.billing_reason
            assert orig.attempt_no == pristine.attempt_no
            assert orig.amount == pristine.amount
            assert orig.subscription_id == pristine.subscription_id
            assert orig.user_id == pristine.user_id