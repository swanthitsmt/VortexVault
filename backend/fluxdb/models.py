from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import BIGINT, JSON, DateTime, Enum, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from fluxdb.db import Base


class JobStatus(str, enum.Enum):
    queued = "queued"
    running = "running"
    paused = "paused"
    completed = "completed"
    failed = "failed"


class UploadStatus(str, enum.Enum):
    initiated = "initiated"
    completed = "completed"
    aborted = "aborted"


class UploadSession(Base):
    __tablename__ = "upload_sessions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status: Mapped[UploadStatus] = mapped_column(Enum(UploadStatus, name="upload_status"), nullable=False, default=UploadStatus.initiated)

    bucket: Mapped[str] = mapped_column(String(128), nullable=False)
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    upload_id: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    total_parts: Mapped[int] = mapped_column(Integer, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class IngestJob(Base):
    __tablename__ = "ingest_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus, name="ingest_job_status"), nullable=False, default=JobStatus.queued)

    source_bucket: Mapped[str] = mapped_column(String(128), nullable=False)
    source_object: Mapped[str] = mapped_column(Text, nullable=False)

    checkpoint_offset: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    source_size_bytes: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)

    processed_lines: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    indexed_docs: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    invalid_lines: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    duplicate_lines: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)

    shard_counts: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    metadata_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_checkpoint_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    merge_job: Mapped["MergeJob | None"] = relationship(back_populates="ingest_job", uselist=False)


class MergeJob(Base):
    __tablename__ = "merge_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus, name="merge_job_status"), nullable=False, default=JobStatus.queued)

    ingest_job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("ingest_jobs.id", ondelete="CASCADE"), nullable=False, unique=True)
    bloom_cardinality_estimate: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    cleaned_objects: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    ingest_job: Mapped[IngestJob] = relationship(back_populates="merge_job")


class ExportJob(Base):
    __tablename__ = "export_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus, name="export_job_status"), nullable=False, default=JobStatus.queued)

    query_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    filter_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    filter_username: Mapped[str | None] = mapped_column(Text, nullable=True)
    line_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=100000)

    object_bucket: Mapped[str] = mapped_column(String(128), nullable=False)
    object_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    exported_rows: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)

    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SearchMetric(Base):
    __tablename__ = "search_metrics"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    query_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    result_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
