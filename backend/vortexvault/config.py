from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "VortexVault v2"
    app_env: str = "local"
    api_host: str = "0.0.0.0"
    api_port: int = 8080

    database_url: str = "postgresql+asyncpg://vortexvault:vortexvault@postgres:5432/vortexvault"
    database_url_sync: str = "postgresql+psycopg://vortexvault:vortexvault@postgres:5432/vortexvault"

    redis_url: str = "redis://redis:6379/0"
    celery_broker_url: str = "redis://redis:6379/1"
    celery_result_backend: str = "redis://redis:6379/2"

    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False
    minio_region: str = "us-east-1"
    minio_bucket_raw: str = "raw-combos"
    minio_bucket_export: str = "exports"

    meili_shards: str = "http://meili-shard-0:7700,http://meili-shard-1:7700,http://meili-shard-2:7700,http://meili-shard-3:7700"
    meili_master_key: str = "change-me"
    meili_index_prefix: str = "combo"
    meili_search_timeout_ms: int = Field(default=1500, ge=200, le=10000)

    ingest_stream_chunk_bytes: int = Field(default=8 * 1024 * 1024, ge=1024 * 256)
    ingest_batch_docs: int = Field(default=25000, ge=1000, le=50000)
    ingest_checkpoint_stride_gb: int = Field(default=20, ge=1, le=50)

    search_default_limit: int = Field(default=100, ge=1, le=1000)
    search_max_limit: int = Field(default=5000, ge=10, le=20000)

    export_tmp_dir: str = "/tmp/vortexvault-exports"
    export_page_size: int = Field(default=5000, ge=1000, le=20000)
    export_presign_ttl_sec: int = Field(default=3600, ge=60, le=86400)

    metrics_retention_days: int = Field(default=14, ge=1, le=90)

    @property
    def meili_hosts(self) -> list[str]:
        hosts = [h.strip() for h in self.meili_shards.split(",") if h.strip()]
        return hosts or ["http://meili-shard-0:7700"]

    @property
    def shard_count(self) -> int:
        return len(self.meili_hosts)

    @property
    def checkpoint_stride_bytes(self) -> int:
        return self.ingest_checkpoint_stride_gb * 1024 * 1024 * 1024


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
