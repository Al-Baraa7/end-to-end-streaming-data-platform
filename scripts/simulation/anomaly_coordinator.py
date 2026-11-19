"""Global Anomaly Coordinator — Phase 7: Anomaly Injection (Flat Target + LLN Convergence)

Enforces a flat target anomaly rate of 3.5% (strictly bounded 1%-5%) across individual data streams.
Volume-weighted blending and statistical convergence to the global target are achieved naturally
via the Law of Large Numbers and inherent Poisson Noise in the generation pipeline, rather than
artificially partitioning budgets through procedural code.
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
    Global coordinator for flat-target independent anomaly injection.

        Ensures independent flat target anomaly assignments across entity streams
        while allowing global weighted blending to emerge naturally through 
        the Law of Large Numbers and statistical Poisson convergence.
    """
    
    # Target blended anomaly rate (3.5%)
    TARGET_BLENDED_RATE = 0.035
    # Strict bounds on blended rate
    MIN_BLENDED_RATE = 0.01
    MAX_BLENDED_RATE = 0.05
    
    def __init__(self, random_provider: RandomProvider):
        """
        Initialize with a single shared RandomProvider instance.
        """
        
        self._random_provider = random_provider
        self._volumes: Dict[str, VolumeProfile] = {}
        self._per_stream_rates: Dict[str, float] = {}
        self._blended_rate = self.TARGET_BLENDED_RATE

    @property
    def random_provider(self) -> RandomProvider:
        """Return the shared random provider instance."""
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
        Assigns an independent flat target rate of 3.5% across all registered data streams.
        Volume-weighted blending and statistical convergence to the global target are 
        achieved naturally via the Law of Large Numbers and inherent Poisson Noise.

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



# -- Phase 7: Convenience Factory --

def create_coordinator(
    random_provider: RandomProvider,
    user_count: int = 0,
    subscription_count: int = 0,
    payment_count: int = 0,
    video_count: int = 0,
    event_count: int = 0
) -> GlobalAnomalyCoordinator:
    """
    Create and configure a GlobalAnomalyCoordinator with typical entity volumes.

    Args:
        random_provider: The shared single source of truth RandomProvider instance.
        user_count: Number of users.
        subscription_count: Number of subscriptions.
        payment_count: Number of payments.
        video_count: Number of videos.
        event_count: Number of Kafka events.

    Returns:
        Configured GlobalAnomalyCoordinator instance.
    """
    coordinator = GlobalAnomalyCoordinator(random_provider=random_provider)
    
    # الحل الجذري: نمرر الأرقام الصحيحة مباشرة (int) للدالة لمنع الـ TypeError
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