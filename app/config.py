from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "VortexVault Search"
    app_host: str = "0.0.0.0"
    app_port: int = Field(default=8000, ge=1, le=65535)
    debug: bool = False
    secret_key: str = "change-me"

    basic_auth_username: str = "admin"
    basic_auth_password: str = "admin123"

    database_url: str = "postgresql+asyncpg://combo:combo@localhost:5432/combo_db"
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    job_workdir: Path = Field(default=Path("./data/jobs"))
    cleaner_flush_every: int = Field(default=5000, ge=1)
    upload_batch_size: int = Field(default=100_000, ge=1000)
    upload_commit_every_batches: int = Field(default=8, ge=1)
    upload_shard_count: int = Field(default=8, ge=1, le=128)
    upload_manifest_enabled: bool = True
    upload_manifest_min_file_mb: int = Field(default=64, ge=1)
    upload_chunk_target_mb: int = Field(default=128, ge=16)
    upload_manifest_parallel_workers: int = Field(default=6, ge=1, le=32)
    upload_auto_tune_workers: bool = True
    upload_auto_tune_max_pg_active: int = Field(default=48, ge=1)
    upload_synchronous_commit: str = Field(default="off")
    merge_batch_size: int = Field(default=250_000, ge=1000)
    merge_commit_every_batches: int = Field(default=4, ge=1)
    merge_drop_staging_on_success: bool = True
    merge_drop_indexes: bool = False
    merge_advisory_lock_key: int = Field(default=424242, ge=1)
    merge_maintenance_work_mem: str = "1GB"
    merge_work_mem: str = "256MB"
    staging_cleanup_enabled: bool = True
    staging_retention_hours: int = Field(default=12, ge=1, le=24 * 30)
    pipeline_cleanup_output_on_success: bool = True
    pipeline_cleanup_job_workdir_on_success: bool = True
    pipeline_cleanup_checkpoints_on_success: bool = True
    max_export_rows: int = Field(default=5_000_000, ge=1)
    search_statement_timeout_ms: int = Field(default=2500, ge=100)
    search_count_timeout_ms: int = Field(default=12000, ge=500)
    dashboard_status_cache_seconds: int = Field(default=10, ge=1, le=300)
    regex_min_chars: int = Field(default=3, ge=1)
    ensure_search_indexes_on_startup: bool = False

    # -- Elasticsearch --
    es_enabled: bool = False
    es_url: str = "http://localhost:9200"
    es_number_of_shards: int = Field(default=3, ge=1)
    es_number_of_replicas: int = Field(default=1, ge=0)
    es_timeout: int = Field(default=30, ge=5)
    es_sync_batch_size: int = Field(default=5000, ge=100)
    es_min_sync_ratio: float = Field(default=0.9, ge=0.0, le=1.0)
    es_sync_check_interval_seconds: int = Field(default=30, ge=1, le=3600)

    @property
    def postgres_dsn(self) -> str:
        return self.database_url.replace("+asyncpg", "").replace("+psycopg", "")

    @property
    def sync_sqlalchemy_database_url(self) -> str:
        if "+asyncpg" in self.database_url:
            return self.database_url.replace("+asyncpg", "+psycopg")
        if self.database_url.startswith("postgresql://"):
            return self.database_url.replace("postgresql://", "postgresql+psycopg://", 1)
        return self.database_url


settings = Settings()
settings.job_workdir.mkdir(parents=True, exist_ok=True)
