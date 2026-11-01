"""
Storage Configuration Module.

Provides storage backend configuration abstraction. Currently supports S3A/MinIO
but designed to be extensible for other backends (GCS, HDFS, ADLS, etc.).
"""
import os
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class StorageConfig:
    """
    Immutable storage backend configuration.
    
    Encapsulates all connection parameters for the object store.
    """
    # Connection
    endpoint: str = "http://minio:9000"
    access_key: str = "minio"
    secret_key: str = "minio123"
    region: str = "us-east-1"
    
    # Behavior
    path_style_access: bool = True
    ssl_enabled: bool = False
    
    # Performance
    max_connections: int = 50
    connection_timeout_ms: int = 5000
    read_timeout_ms: int = 30000
    max_retries: int = 3
    
    # Multipart upload
    multipart_threshold_mb: int = 100
    multipart_chunksize_mb: int = 16
    
    @classmethod
    def from_env(cls) -> "StorageConfig":
        """Create StorageConfig from environment variables."""
        return cls(
            endpoint=os.getenv("STORAGE_ENDPOINT", "http://minio:9000"),
            access_key=os.getenv("STORAGE_ACCESS_KEY", "minio"),
            secret_key=os.getenv("STORAGE_SECRET_KEY", "minio123"),
            region=os.getenv("STORAGE_REGION", "us-east-1"),
            path_style_access=os.getenv("STORAGE_PATH_STYLE", "true").lower() == "true",
            ssl_enabled=os.getenv("STORAGE_SSL_ENABLED", "false").lower() == "true",
            max_connections=int(os.getenv("STORAGE_MAX_CONNECTIONS", "50")),
            connection_timeout_ms=int(os.getenv("STORAGE_CONN_TIMEOUT_MS", "5000")),
            read_timeout_ms=int(os.getenv("STORAGE_READ_TIMEOUT_MS", "30000")),
            max_retries=int(os.getenv("STORAGE_MAX_RETRIES", "3")),
            multipart_threshold_mb=int(os.getenv("STORAGE_MULTIPART_THRESHOLD_MB", "100")),
            multipart_chunksize_mb=int(os.getenv("STORAGE_MULTIPART_CHUNKSIZE_MB", "16")),
        )
    
    def to_spark_configs(self) -> Dict[str, str]:
        """Convert to Spark Hadoop configuration key-value pairs."""
        return {
            "fs.s3a.endpoint": self.endpoint,
            "fs.s3a.access.key": self.access_key,
            "fs.s3a.secret.key": self.secret_key,
            "fs.s3a.path.style.access": str(self.path_style_access).lower(),
            "fs.s3a.connection.ssl.enabled": str(self.ssl_enabled).lower(),
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.connection.maximum": str(self.max_connections),
            "fs.s3a.connection.establish.timeout": str(self.connection_timeout_ms),
            "fs.s3a.connection.timeout": str(self.read_timeout_ms),
            "fs.s3a.attempts.maximum": str(self.max_retries),
            "fs.s3a.multipart.size": str(self.multipart_chunksize_mb * 1024 * 1024),
            "fs.s3a.multipart.threshold": str(self.multipart_threshold_mb * 1024 * 1024),
        }


# Backwards compatibility
def get_minio_config() -> Dict[str, str]:
    """Get MinIO configuration as Spark configs (legacy helper)."""
    return StorageConfig.from_env().to_spark_configs()