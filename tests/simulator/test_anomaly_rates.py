"""
Test Anomaly Injection Rates - R19
Verify that the GlobalAnomalyCoordinator produces per-stream rates within bounds [0.01, 0.05]
and hits the volume-weighted blended target of 3.5%.
"""

import pytest

from scripts.simulation.anomaly_coordinator import GlobalAnomalyCoordinator, create_coordinator


class TestAnomalyInjectionRates:
    """Verify anomaly rates from coordinator are within strict bounds."""

    def test_coordinator_blended_rate_target(self):
        """Global blended rate should be ~3.5% (0.035) with tight tolerance."""
        coordinator = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        assert 0.034 <= coordinator.blended_rate <= 0.036

    def test_coordinator_all_rates_within_bounds(self):
        """Every per-stream rate must be within [0.01, 0.05]."""
        coordinator = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        # Get all rates via get_injector_kwargs
        all_rates = {}
        all_rates.update(coordinator.get_injector_kwargs()["postgres"])
        all_rates.update(coordinator.get_injector_kwargs()["mongo"])
        all_rates.update(coordinator.get_injector_kwargs()["kafka"])
        
        for stream_name, rate in all_rates.items():
            assert 0.01 <= rate <= 0.05

    def test_coordinator_postgres_user_anomaly_rate(self):
        """Postgres user anomaly rate within strict bounds."""
        coordinator = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        pg_rates = coordinator.get_injector_kwargs()["postgres"]
        rate = pg_rates["user_anomaly_rate"]
        assert 0.01 <= rate <= 0.05

    def test_coordinator_postgres_subscription_anomaly_rate(self):
        """Postgres subscription anomaly rate within strict bounds."""
        coordinator = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        pg_rates = coordinator.get_injector_kwargs()["postgres"]
        rate = pg_rates["subscription_anomaly_rate"]
        assert 0.01 <= rate <= 0.05

    def test_coordinator_postgres_payment_anomaly_rate(self):
        """Postgres payment anomaly rate within strict bounds."""
        coordinator = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        pg_rates = coordinator.get_injector_kwargs()["postgres"]
        rate = pg_rates["payment_anomaly_rate"]
        assert 0.01 <= rate <= 0.05

    def test_coordinator_mongo_anomaly_rate(self):
        """MongoDB video anomaly rate within strict bounds."""
        coordinator = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        mongo_rates = coordinator.get_injector_kwargs()["mongo"]
        rate = mongo_rates["anomaly_rate"]
        assert 0.01 <= rate <= 0.05

    def test_coordinator_kafka_anomaly_rate(self):
        """Kafka event anomaly rate within strict bounds."""
        coordinator = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        kafka_rates = coordinator.get_injector_kwargs()["kafka"]
        rate = kafka_rates["anomaly_rate"]
        assert 0.01 <= rate <= 0.05

    def test_coordinator_small_volumes(self):
        """Coordinator handles small volumes correctly (edge case)."""
        coordinator = create_coordinator(
            seed=42,
            user_count=10,
            subscription_count=10,
            payment_count=10,
            video_count=5,
            event_count=50,
        )
        
        # Get all rates via get_injector_kwargs
        all_rates = {}
        all_rates.update(coordinator.get_injector_kwargs()["postgres"])
        all_rates.update(coordinator.get_injector_kwargs()["mongo"])
        all_rates.update(coordinator.get_injector_kwargs()["kafka"])
        
        for stream_name, rate in all_rates.items():
            assert 0.01 <= rate <= 0.05

    def test_coordinator_deterministic(self):
        """Same seed produces same rates (determinism)."""
        coord1 = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        coord2 = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        assert coord1.blended_rate == coord2.blended_rate
        assert coord1.get_injector_kwargs() == coord2.get_injector_kwargs()

    def test_coordinator_shared_random_provider(self):
        """Coordinator provides a shared RandomProvider instance."""
        coordinator = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        from scripts.simulation.random_provider import RandomProvider
        assert isinstance(coordinator.random_provider, RandomProvider)
        
        val1 = coordinator.random_provider.random()
        val2 = coordinator.random_provider.random()
        
        coord2 = create_coordinator(seed=42, user_count=1000, subscription_count=2000)
        val3 = coord2.random_provider.random()
        
        assert val1 == val3