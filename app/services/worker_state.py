from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import FileCheckpoint, JobStatus, ProcessingJob


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_job(session: Session, job_id: UUID) -> ProcessingJob | None:
    stmt = select(ProcessingJob).where(ProcessingJob.id == job_id)
    return session.execute(stmt).scalar_one_or_none()


def get_checkpoint(session: Session, job_id: UUID, file_path: str) -> FileCheckpoint | None:
    stmt = select(FileCheckpoint).where(
        FileCheckpoint.job_id == job_id,
        FileCheckpoint.file_path == file_path,
    )
    return session.execute(stmt).scalar_one_or_none()


def upsert_checkpoint(
    session: Session,
    *,
    job_id: UUID,
    file_path: str,
    encoding: str | None,
    position: int,
    processed_lines: int,
) -> FileCheckpoint:
    checkpoint = get_checkpoint(session, job_id, file_path)
    if checkpoint is None:
        checkpoint = FileCheckpoint(
            job_id=job_id,
            file_path=file_path,
            encoding=encoding,
            position=position,
            processed_lines=processed_lines,
        )
        session.add(checkpoint)
    else:
        checkpoint.encoding = encoding
        checkpoint.position = position
        checkpoint.processed_lines = processed_lines
        checkpoint.updated_at = utcnow()
    return checkpoint


def mark_running(job: ProcessingJob) -> None:
    if job.started_at is None:
        job.started_at = utcnow()
    job.status = JobStatus.running
    job.error_message = None


def mark_failed(job: ProcessingJob, message: str) -> None:
    job.status = JobStatus.failed
    job.error_message = message
    job.finished_at = utcnow()


def mark_completed(job: ProcessingJob) -> None:
    job.status = JobStatus.completed
    job.pause_requested = False
    job.current_file = None
    job.current_line = 0
    job.finished_at = utcnow()


def mark_paused(job: ProcessingJob) -> None:
    job.status = JobStatus.paused
    job.finished_at = None


def refresh_meta(job: ProcessingJob, **extra: Any) -> None:
    current = dict(job.meta or {})
    current.update(extra)
    job.meta = current


def ensure_parent_dir(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
