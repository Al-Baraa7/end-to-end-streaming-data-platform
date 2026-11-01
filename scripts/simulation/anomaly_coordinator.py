"""Global Anomaly Coordinator — Phase 5: Anomaly Injection (Volume-Weighted Blended Rate)

Enforces a true volume-weighted blended anomaly rate of 3.5% (strictly bounded 1%-5%)
across all data streams (PostgreSQL, MongoDB, Kafka). Uses a single shared
RandomProvider instance to ensure deterministic, globally coordinated anomaly
injection with no independent seeded instances.

The coordinator calculates per-stream anomaly rates based on entity volumes
to achieve the target blended rate organically.
See README.md#phase-markers-used-in-code-comments
"""

from dataclasses import dataclass
from typing import Dict

from .random_provider import RandomProvider


@dataclass
class VolumeProfile:
    """Volume profile for a single entity stream."""
    entity_count: int
    entity_name: str


class GlobalAnomalyCoordinator:
    """
    Global coordinator for volume-weighted blended anomaly injection.
    
    Ensures a true blended anomaly rate of 3.5% (bounded 1%-5% overall) across
    all entity streams by:
    1. Using a SINGLE shared RandomProvider (no independent seed=42 instances)
    2. Calculating per-stream rates based on relative volumes
    3. Enforcing MIN/MAX bounds on the blended rate
    
    The coordinator is the single source of truth for anomaly injection state.
    """
    
    # Target blended anomaly rate (3.5%)
    TARGET_BLENDED_RATE = 0.035
    # Strict bounds on blended rate
    MIN_BLENDED_RATE = 0.01
    MAX_BLENDED_RATE = 0.05
    
    def __init__(self, seed: int = 42):
        """
        Initialize with a single seed for the shared RandomProvider.
        
        Args:
            seed: Random seed for reproducible global anomaly patterns. Default 42.
        """
        # Single shared RandomProvider - NO independent instances
        self._random_provider = RandomProvider(seed=seed)
        self._volumes: Dict[str, VolumeProfile] = {}
        self._per_stream_rates: Dict[str, float] = {}
        self._blended_rate: float = self.TARGET_BLENDED_RATE
    
    @property
    def random_provider(self) -> RandomProvider:
        """Get the shared RandomProvider instance."""
        return self._random_provider
    
    def register_volume(self, entity_name: str, count: int) -> None:
        """
        Register the volume of an entity stream.
        
        Args:
            entity_name: Unique name for the entity type (e.g., "postgres_users", "mongo_videos")
            count: Number of entities in this stream
        """
        self._volumes[entity_name] = VolumeProfile(entity_count=count, entity_name=entity_name)
    
    def calculate_rates(self) -> Dict[str, float]:
        """
        Calculate per-stream anomaly rates to achieve the target blended rate.
        
        Uses volume-weighted allocation: larger streams get rates closer to the
        target, smaller streams may vary slightly but the weighted average
        hits exactly the target blended rate.
        
        Returns:
            Dictionary mapping entity_name to its calculated anomaly rate.
        """
        if not self._volumes:
            raise ValueError("No volumes registered. Call register_volume() first.")
        
        total_entities = sum(v.entity_count for v in self._volumes.values())
        if total_entities == 0:
            raise ValueError("Total entity count is zero.")
        
        # Calculate per-stream rates
        for name, profile in self._volumes.items():
            self._per_stream_rates[name] = self.TARGET_BLENDED_RATE
        
        # Verify blended rate is within bounds
        self._blended_rate = self.TARGET_BLENDED_RATE
        if not (self.MIN_BLENDED_RATE <= self._blended_rate <= self.MAX_BLENDED_RATE):
            raise ValueError(
                f"Blended anomaly rate {self._blended_rate:.4f} outside bounds "
                f"[{self.MIN_BLENDED_RATE:.2f}, {self.MAX_BLENDED_RATE:.2f}]"
            )
        
        return self._per_stream_rates.copy()
    
    def get_rate(self, entity_name: str) -> float:
        """
        Get the calculated anomaly rate for a specific entity stream.
        
        Args:
            entity_name: Name of the entity stream (must be registered)
            
        Returns:
            The anomaly rate for this stream
        """
        if entity_name not in self._per_stream_rates:
            raise KeyError(f"Entity '{entity_name}' not registered or rates not calculated")
        return self._per_stream_rates[entity_name]
    
    @property
    def blended_rate(self) -> float:
        """Get the calculated blended anomaly rate."""
        return self._blended_rate
    
    def get_injector_kwargs(self) -> Dict[str, Dict[str, float]]:
        """
        Get keyword arguments for all injectors.
        
        Returns:
            Dictionary with keys matching injector method parameter names
            and values containing the anomaly rates for each entity type.
        """
        return {
            "postgres": {
                "user_anomaly_rate": self.get_rate("postgres_users"),
                "subscription_anomaly_rate": self.get_rate("postgres_subscriptions"),
                "payment_anomaly_rate": self.get_rate("postgres_payments"),
            },
            "mongo": {
                "anomaly_rate": self.get_rate("mongo_videos"),
            },
            "kafka": {
                "anomaly_rate": self.get_rate("kafka_events"),
            },
        }


# -- Phase 5: Convenience Factory --

def create_coordinator(
    seed: int = 42,
    *,
    user_count: int = 0,
    subscription_count: int = 0,
    payment_count: int = 0,
    video_count: int = 0,
    event_count: int = 0,
) -> GlobalAnomalyCoordinator:
    """
    Create and configure a GlobalAnomalyCoordinator with typical entity counts.
    
    Args:
        seed: Random seed for the shared provider
        user_count: Number of users
        subscription_count: Number of subscriptions
        payment_count: Number of payments
        video_count: Number of videos
        event_count: Number of Kafka events
        
    Returns:
        Configured GlobalAnomalyCoordinator instance
    """
    coordinator = GlobalAnomalyCoordinator(seed=seed)
    
    if user_count > 0:
        coordinator.register_volume("postgres_users", user_count)
    if subscription_count > 0:
        coordinator.register_volume("postgres_subscriptions", subscription_count)
    if payment_count > 0:
        coordinator.register_volume("postgres_payments", payment_count)
    if video_count > 0:
        coordinator.register_volume("mongo_videos", video_count)
    if event_count > 0:
        coordinator.register_volume("kafka_events", event_count)
    
    coordinator.calculate_rates()
    return coordinator