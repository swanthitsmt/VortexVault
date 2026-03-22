from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    BIGINT,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class JobType(str, enum.Enum):
    clean = "clean"
    upload = "upload"
    merge = "merge"


class JobStatus(str, enum.Enum):
    pending = "pending"
    queued = "queued"
    running = "running"
    paused = "paused"
    completed = "completed"
    failed = "failed"


class ProcessingJob(Base):
    __tablename__ = "processing_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_type: Mapped[JobType] = mapped_column(Enum(JobType, name="job_type"), nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status"), nullable=False, default=JobStatus.pending
    )

    source_path: Mapped[str] = mapped_column(Text, nullable=False)
    output_file: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    current_file: Mapped[str | None] = mapped_column(Text, nullable=True)
    current_line: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    processed_lines: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    unique_found: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    rows_inserted: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    rows_skipped: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)

    pause_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    celery_task_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    meta: Mapped[dict] = mapped_column(JSONB, default=dict)

    checkpoints: Mapped[list[FileCheckpoint]] = relationship(
        "FileCheckpoint", back_populates="job", cascade="all, delete-orphan"
    )


class FileCheckpoint(Base):
    __tablename__ = "file_checkpoints"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("processing_jobs.id", ondelete="CASCADE"))
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    encoding: Mapped[str | None] = mapped_column(String(32), nullable=True)
    position: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    processed_lines: Mapped[int] = mapped_column(BIGINT, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    job: Mapped[ProcessingJob] = relationship("ProcessingJob", back_populates="checkpoints")

    __table_args__ = (UniqueConstraint("job_id", "file_path", name="uq_checkpoint_job_file"),)


class ComboEntry(Base):
    __tablename__ = "combo_entries"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    password: Mapped[str] = mapped_column(Text, nullable=False)
    digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_combo_url_trgm", "url", postgresql_using="gin", postgresql_ops={"url": "gin_trgm_ops"}),
        Index(
            "idx_combo_username_trgm",
            "username",
            postgresql_using="gin",
            postgresql_ops={"username": "gin_trgm_ops"},
        ),
        Index(
            "idx_combo_password_trgm",
            "password",
            postgresql_using="gin",
            postgresql_ops={"password": "gin_trgm_ops"},
        ),
    )


class ComboKey(Base):
    __tablename__ = "combo_keys"

    digest: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RawIngestEntry(Base):
    __tablename__ = "raw_ingest_entries"

    id: Mapped[int] = mapped_column(BIGINT, primary_key=True, autoincrement=True)
    source_job_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    password: Mapped[str] = mapped_column(Text, nullable=False)
    digest: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_raw_ingest_created_id", "created_at", "id"),
        Index("idx_raw_ingest_source_job", "source_job_id"),
    )
