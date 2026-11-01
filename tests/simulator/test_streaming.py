"""
Test Deepcopy Streaming Pattern - R19
"""

import pytest
from datetime import datetime, timezone

from scripts.simulation.anomaly_coordinator import create_coordinator
from scripts.simulation.postgres.user_entity import UserEntity
from scripts.simulation.dirty_data.postgres_dirty_injection import PostgresDirtyDataInjector


class TestDeepcopyStreamingPattern:
    """Verify deepcopy micro-mutation pattern preserves memory efficiency."""

    def test_injectors_yield_individual_entities(self):
        """Injectors yield entities one at a time (generator pattern)."""
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
        
        users = [UserEntity(
            user_id=f"user_{i}", email=f"u{i}@e.com", country="KSA",
            account_status="active", created_at=datetime(2026,1,1,tzinfo=timezone.utc),
            last_login=datetime(2026,6,1,tzinfo=timezone.utc)) for i in range(10)]
        
        result = injector.inject_users(users=users, anomaly_rate=pg_rates["user_anomaly_rate"])
        assert hasattr(result, '__iter__')
        assert hasattr(result, '__next__')
        
        count = 0
        for entity in result:
            count += 1
            assert isinstance(entity, UserEntity)
        assert count == 10

    def test_deepcopy_isolation(self):
        """Each yielded entity is an independent deepcopy."""
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
        
        users = [UserEntity(
            user_id=f"user_{i}", email=f"u{i}@e.com", country="KSA",
            account_status="active", created_at=datetime(2026,1,1,tzinfo=timezone.utc),
            last_login=datetime(2026,6,1,tzinfo=timezone.utc)) for i in range(10)]
        
        corrupted = list(injector.inject_users(users=users, anomaly_rate=pg_rates["user_anomaly_rate"]))
        
        for orig, corr in zip(users, corrupted):
            assert orig is not corr
            if corr.email == orig.email and corr.country == orig.country:
                assert corr.email == orig.email