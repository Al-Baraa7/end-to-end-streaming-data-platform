"""MongoDB Dirty Data Injector — Phase 7: Anomaly Injection (MongoDB)

Uses a shared RandomProvider for stratified, deterministic anomaly injection
at a volume-weighted blended 3.5% rate (strictly bounded 1%-5% overall)
across video entities. The coordinator enforces global budget.

Applies deepcopy() micro-mutations on individual entity objects ONLY after
they pass clean verification. The pristine master object remains untouched
in memory for subsequent Kafka session/event telemetry.
See README.md#phase-markers-used-in-code-comments
"""

from copy import deepcopy
from typing import Iterator, List

from ..random_provider import RandomProvider

# Valid content types and statuses from VideoSimulation
VALID_CONTENT_TYPES = ("live", "highlight", "replay")
VALID_STATUSES = ("scheduled", "live", "ended")
VALID_COMPETITIONS = ("RSL", "KINGS_CUP", "AFC_CHAMPIONS_LEAGUE", "WORLD_CUP")


class MongoDirtyDataInjector:
    """
    Streaming row-by-row dirty data injector for MongoDB video entities.
    
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

    def _inject_video_anomalies(self, video: dict) -> None:
        """Inject anomalies into a single video entity (mutates the deepcopy)."""
        anomaly_type = self.random.randint(1, 8)

        if anomaly_type == 1:
            # Null mandatory field: title
            video["title"] = None
        elif anomaly_type == 2:
            # Invalid duration_sec: zero or negative (violates R5)
            video["duration_sec"] = self.random.randint(-100, 0)
        elif anomaly_type == 3:
            # Invalid duration_sec: exceeds match window (violates R5)
            # Set to an unreasonably large value
            video["duration_sec"] = 20000
        elif anomaly_type == 4:
            # Invalid content_type enum
            video["content_type"] = "INVALID_TYPE"
        elif anomaly_type == 5:
            # Referential orphan: match_id = None for live content
            video["content_type"] = "live"
            video["match_id"] = None
            video["live_start_at"] = None
            video["live_end_at"] = None
        elif anomaly_type == 6:
            # Status contradiction: live content with ended status but future window
            video["content_type"] = "live"
            video["status"] = "ended"
        elif anomaly_type == 7:
            # Metric invariant break: likes > views
            video["stats"]["views"] = 150
            video["stats"]["likes"] = self.random.randint(151, 500)
        elif anomaly_type == 8:
            # Missing competition metadata (R6)
            video["competition"] = {}
            video["tags"] = []

    def inject_videos(self, videos: List[dict], anomaly_rate: float) -> Iterator[dict]:
        """
        Stream videos with anomalies injected at the coordinator-calculated rate.
        
        Yields deep-copied entities (pristine or corrupted) one at a time
        to avoid memory inflation. Pristine master objects remain untouched.
        """
        for video in videos:
            video_copy = deepcopy(video)

            if self._should_inject(anomaly_rate):
                self._inject_video_anomalies(video_copy)

            yield video_copy

    def inject(self, *, videos: List[dict], anomaly_rate: float) -> Iterator[dict]:
        """
        Inject anomalies into video entities using coordinator rate.
        
        Returns an iterator for streaming processing.
        The pristine master list remains untouched in memory.
        """
        return self.inject_videos(videos, anomaly_rate)