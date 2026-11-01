"""
Centralized Spark Session Management Module.

Provides a reusable, parameterized SparkSession builder that abstracts all
infrastructure configuration (master URLs, memory, parallelism, S3A/MinIO credentials)
away from pipeline jobs. Supports environment-specific configuration via config files
or environment variables.
"""
import os
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
@dataclass(frozen=True)
class SparkConfig:
    """
    Immutable configuration for Spark session initialization.
    
    All infrastructure parameters are externalized to support dev/staging/prod
    deployments without code changes.
    """
    app_name: str
    master_url: str = "spark://spark-master:7077"
    driver_memory: str = "2g"
    executor_memory: str = "2g"
    executor_cores: int = 2
    num_executors: int = 2
    default_parallelism: int = 8
    shuffle_partitions: int = 8
    adaptive_query_execution: bool = True
    auto_broadcast_join_threshold: str = "100m"
    parquet_vectorized_reader: bool = True
    parquet_nanos_as_long: bool = True
    
    # S3A / MinIO Configuration
    s3a_endpoint: str = "http://minio:9000"
    s3a_access_key: str = "minio"
    s3a_secret_key: str = "minio123"
    s3a_path_style_access: bool = True
    s3a_impl: str = "org.apache.hadoop.fs.s3a.S3AFileSystem"
    s3a_connection_ssl_enabled: bool = False
    
    # Additional Spark configs
    extra_configs: Dict[str, str] = field(default_factory=dict)
    
    @classmethod
    def from_env(cls, app_name: str, env: Optional[str] = None) -> "SparkConfig":
        """
        Create SparkConfig from environment variables with sensible defaults.
        
        Environment variables (all optional):
        - SPARK_MASTER_URL
        - SPARK_DRIVER_MEMORY
        - SPARK_EXECUTOR_MEMORY
        - SPARK_EXECUTOR_CORES
        - SPARK_NUM_EXECUTORS
        - SPARK_DEFAULT_PARALLELISM
        - SPARK_SHUFFLE_PARTITIONS
        - S3A_ENDPOINT
        - S3A_ACCESS_KEY
        - S3A_SECRET_KEY
        """
        env = env or os.getenv("DEPLOY_ENV", "development")
        
        # Environment-specific defaults
        env_defaults = {
            "development": {
                "master_url": "local[*]",
                "driver_memory": "2g",
                "executor_memory": "2g",
                "num_executors": 1,
            },
            "staging": {
                "master_url": "spark://spark-master:7077",
                "driver_memory": "4g",
                "executor_memory": "4g",
                "num_executors": 2,
            },
            "production": {
                "master_url": "spark://spark-master:7077",
                "driver_memory": "8g",
                "executor_memory": "8g",
                "num_executors": 4,
            }
        }
        
        defaults = env_defaults.get(env, env_defaults["development"])
        
        return cls(
            app_name=app_name,
            master_url=os.getenv("SPARK_MASTER_URL", defaults["master_url"]),
            driver_memory=os.getenv("SPARK_DRIVER_MEMORY", defaults["driver_memory"]),
            executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", defaults["executor_memory"]),
            executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "2")),
            num_executors=int(os.getenv("SPARK_NUM_EXECUTORS", str(defaults["num_executors"]))),
            default_parallelism=int(os.getenv("SPARK_DEFAULT_PARALLELISM", "8")),
            shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")),
            s3a_endpoint=os.getenv("S3A_ENDPOINT", "http://minio:9000"),
            s3a_access_key=os.getenv("S3A_ACCESS_KEY", "minio"),
            s3a_secret_key=os.getenv("S3A_SECRET_KEY", "minio123"),
        )
def get_spark_session(
    app_name: str,
    config: Optional[SparkConfig] = None,
    env: Optional[str] = None,
    **overrides: Any
) -> SparkSession:
    """
    Get or create a configured SparkSession.
    
    This is the single entry point for all pipeline jobs to obtain a Spark session.
    It ensures consistent configuration across all jobs and supports environment-
    specific overrides.
    
    Args:
        app_name: Unique application name for Spark UI and logging
        config: Optional SparkConfig object. If not provided, built from env vars.
        env: Deployment environment (development/staging/production). Used if config not provided.
        **overrides: Additional Spark config key-value pairs to override defaults.
    
    Returns:
        Configured SparkSession instance.
    
    Example:
        >>> spark = get_spark_session("postgres_silver_job", env="production")
        >>> # or with custom config
        >>> config = SparkConfig(app_name="my_job", driver_memory="4g")
        >>> spark = get_spark_session("my_job", config=config)
    """
    if config is None:
        config = SparkConfig.from_env(app_name, env)
    
    # Apply any runtime overrides
    if overrides:
        config = SparkConfig(
            **{**config.__dict__, **overrides}
        )
    
    builder = SparkSession.builder.appName(config.app_name)
    
    # Master and cluster config
    builder = builder.master(config.master_url)
    builder = builder.config("spark.driver.memory", config.driver_memory)
    builder = builder.config("spark.executor.memory", config.executor_memory)
    builder = builder.config("spark.executor.cores", str(config.executor_cores))
    builder = builder.config("spark.executor.instances", str(config.num_executors))
    
    # Parallelism and shuffle tuning
    builder = builder.config("spark.default.parallelism", str(config.default_parallelism))
    builder = builder.config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
    builder = builder.config("spark.sql.adaptive.enabled", str(config.adaptive_query_execution).lower())
    builder = builder.config("spark.sql.autoBroadcastJoinThreshold", config.auto_broadcast_join_threshold)
    
    # Parquet optimization
    builder = builder.config("spark.sql.parquet.enableVectorizedReader", str(config.parquet_vectorized_reader).lower())
    builder = builder.config("spark.sql.legacy.parquet.nanosAsLong", str(config.parquet_nanos_as_long).lower())
    
    # S3A / MinIO configuration
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", config.s3a_endpoint)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", config.s3a_access_key)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", config.s3a_secret_key)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", str(config.s3a_path_style_access).lower())
    builder = builder.config("spark.hadoop.fs.s3a.impl", config.s3a_impl)
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.s3a_connection_ssl_enabled).lower())
    
    # Additional configs for S3A stability
    builder = builder.config("spark.hadoop.fs.s3a.attempts.maximum", "10")
    builder = builder.config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    builder = builder.config("spark.hadoop.fs.s3a.connection.timeout", "10000")
    
    # Apply extra configs
    for key, value in config.extra_configs.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


class SparkSessionManager:
    """
    Context manager for Spark session lifecycle management.
    
    Ensures proper cleanup of Spark resources even if jobs fail.
    
    Example:
        >>> with SparkSessionManager("my_job") as spark:
        ...     df = spark.read.parquet("s3a://bucket/path")
        ...     df.write.parquet("s3a://bucket/output")
    """
    
    def __init__(self, app_name: str, config: Optional[SparkConfig] = None, env: Optional[str] = None):
        self.app_name = app_name
        self.config = config
        self.env = env
        self.spark: Optional[SparkSession] = None
    
    def __enter__(self) -> SparkSession:
        self.spark = get_spark_session(self.app_name, self.config, self.env)
        return self.spark
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.spark is not None:
            self.spark.stop()
            self.spark = None