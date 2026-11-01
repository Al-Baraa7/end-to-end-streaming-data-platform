"""
Test Kafka Anomaly Injection Rates - R19
"""

import pytest
from datetime import datetime, timezone

from scripts.simulation.anomaly_coordinator import create_coordinator
from scripts.simulation.dirty_data.kafka_dirty_injection import KafkaDirtyDataInjector
from scripts.simulation.kafka.interaction_entity import InteractionEntity


class TestKafkaAnomalyRates:
    """Test Kafka anomaly rates."""

    def test_kafka_event_anomaly_rate(self):
        """Kafka event anomaly injection rate within bounds."""
        # Use coordinator to get shared RandomProvider and proper rate
        coordinator = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        kafka_rates = coordinator.get_injector_kwargs()["kafka"]
        injector = KafkaDirtyDataInjector(random_provider=coordinator.random_provider)
        
        events = []
        for i in range(10000):
            events.append(InteractionEntity(
                event_id=f"evt_{i}",
                user_id=f"user_{i}",
                video_id=f"vid_{i}",
                interaction_type="play",
                event_timestamp=datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
                watch_time_sec=300,
                device_type="mobile",
            ))

        corrupted = list(injector.inject(events=events, anomaly_rate=kafka_rates["anomaly_rate"]))
        
        anomaly_count = sum(1 for e in corrupted 
                           if e.watch_time_sec is None
                           or e.interaction_type == "INVALID_TYPE"
                           or e.event_timestamp < datetime(2026, 1, 1, tzinfo=timezone.utc)
                           or e.event_timestamp > datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
                           or e.device_type == "INVALID_DEVICE"
                           or e.watch_time_sec > 5000
                           or e.user_id == ""
                           or e.video_id == "")
        
        rate = anomaly_count / len(events)
        assert 0.01 <= rate <= 0.05, f"Event anomaly rate {rate:.4f} outside bounds [1%, 5%]"