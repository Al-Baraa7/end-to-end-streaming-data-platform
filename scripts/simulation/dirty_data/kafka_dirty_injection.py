"""Kafka Dirty Data Injector — Phase 7: Anomaly Injection (Kafka)

Uses a shared RandomProvider for stratified, deterministic anomaly injection
at a volume-weighted blended 3.5% rate (strictly bounded 1%-5% overall)
across Kafka event entities. The coordinator enforces global budget.

Applies deepcopy() micro-mutations on individual entity objects ONLY after
they pass clean verification. The pristine master object remains untouched
in memory for subsequent Kafka session/event telemetry.
See README.md#phase-markers-used-in-code-comments
"""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Iterator, List

from ..random_provider import RandomProvider

# Valid interaction types and device types from interaction_factory
VALID_INTERACTION_TYPES = ("play", "pause", "seek", "like", "complete")
VALID_DEVICE_TYPES = ("mobile", "desktop", "tablet", "tv")


class KafkaDirtyDataInjector:
    """
    Streaming row-by-row dirty data injector for Kafka event entities.
    
    Uses a SHARED RandomProvider (injected by coordinator) to inject anomalies
    at rates calculated by the global volume coordinator to achieve a
    volume-weighted blended 3.5% rate (bounded 1%-5% overall).
    Each entity is deep-copied before mutation to preserve the pristine master.
    """

    def __init__(self, random_provider: RandomProvider):
        """
        Initialize with a SHARED RandomProvider for deterministic anomaly injection.
        
        Args:
            random_provider: Shared RandomProvider instance from GlobalAnomalyCoordinator.
        """
        self.random = random_provider

    def _should_inject(self, anomaly_rate: float) -> bool:
        """Determine if an anomaly should be injected based on the rate."""
        return self.random.random() < anomaly_rate

    def _inject_event_anomalies(self, event) -> None:
        """Inject anomalies into a single event entity (mutates the deepcopy)."""
        anomaly_type = self.random.randint(1, 6)  # Reduced from 7: ID mutation (type 7) disabled

        if anomaly_type == 1:
            # Null mandatory field: watch_time_sec
            event.watch_time_sec = None
        elif anomaly_type == 2:
            # Invalid interaction_type enum
            event.interaction_type = "INVALID_TYPE"
        elif anomaly_type == 3:
            # Temporal skew: event_timestamp before simulation start (2026-01-01)
            event.event_timestamp = datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        elif anomaly_type == 4:
            # Temporal skew: event_timestamp after simulation end (2026-12-31)
            event.event_timestamp = datetime(2027, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        elif anomaly_type == 5:
            # Invalid device_type enum
            event.device_type = "INVALID_DEVICE"
        elif anomaly_type == 6:
            # Metric invariant break: watch_time_sec > video duration (if play)
            if event.interaction_type == "play":
                event.watch_time_sec = 10000  # Unreasonably large
        # DISABLED: anomaly_type == 7 (original) - Referential orphan: user_id/video_id mutation
        # elif anomaly_type == 7:
        #     # Referential orphan: user_id or video_id that doesn't exist
        #     if self.random.random() < 0.5:
        #         event.user_id = ""
        #     else:
        #         event.video_id = ""

    def inject_events(self, events: List, anomaly_rate: float) -> Iterator:
        """
        Stream events with anomalies injected at the coordinator-calculated rate.
        
        Yields deep-copied entities (pristine or corrupted) one at a time
        to avoid memory inflation. Pristine master objects remain untouched.
        """
        for event in events:
            event_copy = deepcopy(event)

            if self._should_inject(anomaly_rate):
                self._inject_event_anomalies(event_copy)

            yield event_copy

    def inject(self, *, events: List, anomaly_rate: float) -> Iterator:
        """
        Inject anomalies into Kafka event entities using coordinator rate.
        
        Returns an iterator for streaming processing.
        The pristine master list remains untouched in memory.
        """
        return self.inject_events(events, anomaly_rate)