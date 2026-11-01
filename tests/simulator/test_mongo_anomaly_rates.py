"""
Test MongoDB Anomaly Injection Rates - R19
"""

import pytest
from datetime import datetime, timezone

from scripts.simulation.anomaly_coordinator import create_coordinator
from scripts.simulation.dirty_data.mongo_dirty_injection import MongoDirtyDataInjector


class TestMongoAnomalyRates:
    """Test MongoDB anomaly rates."""

    def test_mongo_video_anomaly_rate(self):
        """Video anomaly injection rate within bounds."""
        # Use coordinator to get shared RandomProvider and proper rate
        coordinator = create_coordinator(
            seed=42,
            user_count=1000,
            subscription_count=2000,
            payment_count=4000,
            video_count=500,
            event_count=50000,
        )
        
        mongo_rates = coordinator.get_injector_kwargs()["mongo"]
        injector = MongoDirtyDataInjector(random_provider=coordinator.random_provider)
        
        videos = []
        for i in range(10000):
            videos.append({
                "video_id": f"vid_{i}",
                "title": f"Video {i}",
                "content_type": "highlight",
                "duration_sec": 300,
                "match_id": f"match_{i}",
                "status": "ended",
                "stats": {"views": 100, "likes": 10},
                "competition": {"id": "RSL", "name": "RSL", "teams": ["A", "B"]},
                "tags": ["football"],
                "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            })

        corrupted = list(injector.inject(videos=videos, anomaly_rate=mongo_rates["anomaly_rate"]))
        
        anomaly_count = sum(1 for v in corrupted 
                           if v["title"] is None
                           or v["duration_sec"] <= 0
                           or v["duration_sec"] > 10000
                           or v["content_type"] == "INVALID_TYPE"
                           or v["match_id"] is None
                           or v["status"] == "ended" and v["content_type"] == "live"
                           or v["stats"]["likes"] > v["stats"]["views"]
                           or v["competition"] == {})
        
        rate = anomaly_count / len(videos)
        assert 0.01 <= rate <= 0.05, f"Video anomaly rate {rate:.4f} outside bounds [1%, 5%]"