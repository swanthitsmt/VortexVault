from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class PresignUploadRequest(BaseModel):
    object_name: str = Field(min_length=3, max_length=512)

    @field_validator("object_name")
    @classmethod
    def validate_object_name(cls, value: str) -> str:
        cleaned = value.strip()
        if ".." in cleaned or "\\" in cleaned or cleaned.startswith("/") or cleaned.endswith("/"):
            raise ValueError("Invalid object name")
        return cleaned


class PresignUploadResponse(BaseModel):
    bucket: str
    object_key: str
    put_url: str


class MultipartInitRequest(BaseModel):
    object_name: str = Field(min_length=3, max_length=512)
    total_parts: int = Field(ge=1, le=50000)

    @field_validator("object_name")
    @classmethod
    def validate_multipart_object_name(cls, value: str) -> str:
        cleaned = value.strip()
        if ".." in cleaned or "\\" in cleaned or cleaned.startswith("/") or cleaned.endswith("/"):
            raise ValueError("Invalid object name")
        return cleaned


class MultipartInitResponse(BaseModel):
    session_id: UUID
    bucket: str
    object_key: str
    upload_id: str


class MultipartPartRequest(BaseModel):
    session_id: UUID
    part_number: int = Field(ge=1, le=50000)


class MultipartPartResponse(BaseModel):
    part_number: int
    presigned_url: str


class MultipartCompletePart(BaseModel):
    etag: str
    part_number: int


class MultipartCompleteRequest(BaseModel):
    session_id: UUID
    parts: list[MultipartCompletePart]


class IngestCreateRequest(BaseModel):
    source_bucket: str = Field(min_length=3, max_length=63, pattern=r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$")
    source_object: str = Field(min_length=3, max_length=512)
    auto_merge: bool = True

    @field_validator("source_object")
    @classmethod
    def validate_source_object(cls, value: str) -> str:
        cleaned = value.strip()
        if ".." in cleaned or "\\" in cleaned:
            raise ValueError("Invalid source object")
        return cleaned


class IngestJobResponse(BaseModel):
    id: UUID
    status: str
    source_bucket: str
    source_object: str
    checkpoint_offset: int
    source_size_bytes: int
    processed_lines: int
    indexed_docs: int
    invalid_lines: int
    duplicate_lines: int
    shard_counts: dict[str, Any]
    metadata_json: dict[str, Any]
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    last_checkpoint_at: datetime | None
    finished_at: datetime | None


class MergeCreateRequest(BaseModel):
    ingest_job_id: UUID


class MergeJobResponse(BaseModel):
    id: UUID
    status: str
    ingest_job_id: UUID
    bloom_cardinality_estimate: int
    cleaned_objects: int
    notes: str | None
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class SearchQueryRequest(BaseModel):
    query: str = Field(default="", max_length=512)
    prefix: bool = True
    typo_tolerance: bool = True
    limit: int = Field(default=100, ge=1, le=5000)
    filter_url: str | None = Field(default=None, max_length=512)
    filter_username: str | None = Field(default=None, max_length=512)


class SearchHit(BaseModel):
    id: str
    url: str
    username: str
    password: str
    score: float = 0.0
    shard: int


class SearchQueryResponse(BaseModel):
    took_ms: float
    total_hits: int
    hits: list[SearchHit]


class ExportCreateRequest(BaseModel):
    query: str = Field(default="", max_length=512)
    filter_url: str | None = Field(default=None, max_length=512)
    filter_username: str | None = Field(default=None, max_length=512)
    line_limit: int = Field(default=100000, ge=1, le=50000000)


class ExportJobResponse(BaseModel):
    id: UUID
    status: str
    object_key: str | None
    exported_rows: int
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class DashboardResponse(BaseModel):
    active_ingest_jobs: int
    active_merge_jobs: int
    active_export_jobs: int
    total_completed_ingest: int
    dedupe_cardinality_estimate: int
    shard_count: int
