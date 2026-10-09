# config.py - Centralized configuration

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class DatabaseConfig:
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    name: str = os.getenv("DB_NAME", "jobqueue2")
    user: str = os.getenv("DB_USER", "postgres")
    password: str = os.getenv("DB_PASSWORD", "postgres")
    min_connections: int = 2
    max_connections: int = 10

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


@dataclass
class QueueConfig:
    # Polling
    poll_interval_sec: float = 1.0
    batch_size: int = int(os.getenv("BATCH_SIZE", "10"))  # jobs to claim per poll cycle

    # Leasing
    lease_duration_sec: int = 30
    lease_renewal_interval_sec: int = 10

    # Retries
    default_max_attempts: int = 3
    default_retry_delay_sec: int = 5

    # Recovery
    recovery_interval_sec: float = 15.0

    # Heartbeat
    heartbeat_interval_sec: float = 5.0
    worker_dead_after_sec: float = 30.0

    # Metrics
    metrics_snapshot_interval_sec: float = 5.0

    # Queues this worker listens to
    queue_names: List[str] = field(default_factory=lambda: ["default"])


@dataclass
class AppConfig:
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    num_workers: int = int(os.getenv("NUM_WORKERS", "3"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


# Singleton
config = AppConfig()