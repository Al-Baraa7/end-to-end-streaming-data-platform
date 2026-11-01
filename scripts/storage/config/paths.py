# """
# Path Builder Module for Bronze, Silver, Gold Layers.

# Provides clean, consistent path construction for all storage layers with proper
# partitioning strategies. Eliminates hardcoded paths and string concatenation
# scattered across notebooks.
# """
# from dataclasses import dataclass
# from enum import Enum
# from typing import Optional
# import os


# class Layer(str, Enum):
#     """Storage layer enumeration."""
#     BRONZE = "bronze"
#     SILVER = "silver"
#     GOLD = "gold"


# class SourceSystem(str, Enum):
#     """Source system enumeration."""
#     POSTGRES = "postgres"
#     KAFKA = "kafka"
#     MONGO = "mongo"


# @dataclass(frozen=True)
# class PathBuilder:
#     """
#     Builds consistent S3A paths for all pipeline layers.
    
#     Centralizes path logic to ensure:
#     - Consistent partitioning strategy
#     - No hardcoded bucket names in jobs
#     - Easy migration between environments
#     - Clear separation of concerns by layer
    
#     Bronze: Partitioned by ingestion_date for incremental loads
#     Silver: Flat per-table (or partitioned by business date for events)
#     Gold: Star schema directories (dim_*, fact_*)
#     """
    
#     # Bucket names - can be overridden via env vars
#     bronze_bucket: str = "end-to-end-streaming-data-platform-bronze"
#     silver_bucket: str = "end-to-end-streaming-data-platform-silver"
#     gold_bucket: str = "end-to-end-streaming-data-platform-gold"
    
#     # Protocol
#     protocol: str = "s3a://"
    
#     @classmethod
#     def from_env(cls) -> "PathBuilder":
#         """Create PathBuilder from environment variables."""
#         return cls(
#             bronze_bucket=os.getenv("BRONZE_BUCKET", "end-to-end-streaming-data-platform-bronze"),
#             silver_bucket=os.getenv("SILVER_BUCKET", "end-to-end-streaming-data-platform-silver"),
#             gold_bucket=os.getenv("GOLD_BUCKET", "end-to-end-streaming-data-platform-gold"),
#             protocol=os.getenv("STORAGE_PROTOCOL", "s3a://"),
#         )
    
#     # ==================== BRONZE PATHS ====================
    
#     def bronze_base(self, source: SourceSystem) -> str:
#         """Base bronze path for a source system."""
#         return f"{self.protocol}{self.bronze_bucket}/{source.value}"
    
#     def bronze_partitioned(
#         self,
#         source: SourceSystem,
#         ingestion_date: str,
#         table: str
#     ) -> str:
#         """
#         Bronze path with ingestion_date partitioning.
        
#         Example: s3a://bucket/bronze/postgres/ingestion_date=2026-09-30/users.parquet
#         """
#         base = self.bronze_base(source)
#         return f"{base}/ingestion_date={ingestion_date}/{table}.parquet"
    
#     def bronze_kafka_streaming(self, ingestion_date: str) -> str:
#         """Kafka bronze streaming base path (checkpoint-aware)."""
#         return f"{self.protocol}{self.bronze_bucket}/kafka/ingestion_data={ingestion_date}"
    
#     def bronze_kafka_checkpoint(self) -> str:
#         """Kafka bronze streaming checkpoint path."""
#         return f"{self.protocol}{self.bronze_bucket}/checkpoints/kafka_to_bronze"
    
#     # ==================== SILVER PATHS ====================
    
#     def silver_base(self, source: SourceSystem) -> str:
#         """Base silver path for a source system."""
#         return f"{self.protocol}{self.silver_bucket}/{source.value}"
    
#     def silver_table(self, source: SourceSystem, table: str) -> str:
#         """
#         Silver table path (flat, no partitioning for Postgres/Mongo).
        
#         Example: s3a://bucket/silver/postgres/users.parquet
#         """
#         return f"{self.silver_base(source)}/{table}.parquet"
    
#     def silver_events(self) -> str:
#         """Kafka events silver path."""
#         return f"{self.protocol}{self.silver_bucket}/kafka/events_silver.parquet"
    
#     def silver_videos(self) -> str:
#         """Mongo videos silver path."""
#         return f"{self.protocol}{self.silver_bucket}/mongo/videos_silver.parquet"
    
#     # ==================== GOLD PATHS ====================
    
#     def gold_base(self) -> str:
#         """Base gold path."""
#         return f"{self.protocol}{self.gold_bucket}"
    
#     def gold_dimension(self, name: str) -> str:
#         """Gold dimension table path."""
#         return f"{self.gold_base()}/dim_{name}"
    
#     def gold_fact(self, name: str) -> str:
#         """Gold fact table path."""
#         return f"{self.gold_base()}/fact_{name}"
    
#     # ==================== UTILITY ====================
    
#     def resolve_layer(self, layer: Layer) -> str:
#         """Get bucket name for a layer."""
#         mapping = {
#             Layer.BRONZE: self.bronze_bucket,
#             Layer.SILVER: self.silver_bucket,
#             Layer.GOLD: self.gold_bucket,
#         }
#         return mapping[layer]


# # Singleton instance for convenience
# _default_builder: Optional[PathBuilder] = None


# def get_path_builder() -> PathBuilder:
#     """Get the default path builder instance (singleton)."""
#     global _default_builder
#     if _default_builder is None:
#         _default_builder = PathBuilder.from_env()
#     return _default_builder

"""
Path Builder Module for Bronze, Silver, Gold Layers.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Optional
import os

class Layer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

class SourceSystem(str, Enum):
    POSTGRES = "postgres"
    KAFKA = "kafka"
    MONGO = "mongo"

@dataclass(frozen=True)
class PathBuilder:
    bronze_bucket: str = "end-to-end-streaming-data-platform-bronze"
    silver_bucket: str = "end-to-end-streaming-data-platform-silver"
    gold_bucket: str = "end-to-end-streaming-data-platform-gold"
    protocol: str = "s3a://"
    
    @classmethod
    def from_env(cls) -> "PathBuilder":
        return cls(
            bronze_bucket=os.getenv("BRONZE_BUCKET", "end-to-end-streaming-data-platform-bronze"),
            silver_bucket=os.getenv("SILVER_BUCKET", "end-to-end-streaming-data-platform-silver"),
            gold_bucket=os.getenv("GOLD_BUCKET", "end-to-end-streaming-data-platform-gold"),
            protocol=os.getenv("STORAGE_PROTOCOL", "s3a://"),
        )
    
    # ==================== BRONZE PATHS ====================
    
    def bronze_base(self, source: SourceSystem) -> str:
        return f"{self.protocol}{self.bronze_bucket}/{source.value}"
    
    def bronze_partitioned(
        self,
        source: SourceSystem,
        ingestion_date: str,
        table: str
    ) -> str:
        """ Example: s3a://.../bronze/postgres/ingestion_date=2026-10-31/users.parquet """
        base = self.bronze_base(source)
        return f"{base}/ingestion_date={ingestion_date}/{table}.parquet"
    
    def bronze_kafka_streaming(self, ingestion_date: str) -> str:
        
        return f"{self.protocol}{self.bronze_bucket}/kafka/ingestion_date={ingestion_date}"
    
    def bronze_kafka_checkpoint(self) -> str:
        return f"{self.protocol}{self.bronze_bucket}/checkpoints/kafka_to_bronze"
    
    # ==================== SILVER PATHS ====================
    
    def silver_base(self, source: SourceSystem) -> str:
        return f"{self.protocol}{self.silver_bucket}/{source.value}"
    
    def silver_table(self, source: SourceSystem, table: str) -> str:
        return f"{self.silver_base(source)}/{table}.parquet"
    
    def silver_events(self) -> str:
        return f"{self.protocol}{self.silver_bucket}/kafka/events_silver.parquet"
    
    def silver_videos(self) -> str:
        return f"{self.protocol}{self.silver_bucket}/mongo/videos_silver.parquet"
    
    # ==================== GOLD PATHS ====================
    
    def gold_base(self) -> str:
        return f"{self.protocol}{self.gold_bucket}"
    
    def gold_dimension(self, name: str) -> str:
        return f"{self.gold_base()}/dim_{name}"
    
    def gold_fact(self, name: str) -> str:
        return f"{self.gold_base()}/fact_{name}"
    
    def resolve_layer(self, layer: Layer) -> str:
        mapping = {
            Layer.BRONZE: self.bronze_bucket,
            Layer.SILVER: self.silver_bucket,
            Layer.GOLD: self.gold_bucket,
        }
        return mapping[layer]

_default_builder: Optional[PathBuilder] = None

def get_path_builder() -> PathBuilder:
    global _default_builder
    if _default_builder is None:
        _default_builder = PathBuilder.from_env()
    return _default_builder