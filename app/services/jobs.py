from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import JobStatus, JobType, ProcessingJob


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def create_job(
    session: AsyncSession,
    *,
    job_type: JobType,
    source_path: str,
    output_file: str | None = None,
    meta: dict | None = None,
) -> ProcessingJob:
    job = ProcessingJob(
        job_type=job_type,
        status=JobStatus.queued,
        source_path=source_path,
        output_file=output_file,
        meta=meta or {},
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def list_jobs(session: AsyncSession, *, limit: int = 100) -> list[ProcessingJob]:
    stmt = select(ProcessingJob).order_by(ProcessingJob.created_at.desc()).limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars())


async def get_job(session: AsyncSession, job_id: uuid.UUID) -> ProcessingJob | None:
    stmt = select(ProcessingJob).where(ProcessingJob.id == job_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def request_pause(session: AsyncSession, job: ProcessingJob) -> ProcessingJob:
    job.pause_requested = True
    if job.status in (JobStatus.running, JobStatus.queued):
        job.status = JobStatus.paused
    await session.commit()
    await session.refresh(job)
    return job


async def request_resume(session: AsyncSession, job: ProcessingJob) -> ProcessingJob:
    job.pause_requested = False
    if job.status in (JobStatus.paused, JobStatus.failed):
        job.status = JobStatus.queued
        job.error_message = None
        job.finished_at = None
        if job.started_at is None:
            job.started_at = utcnow()
    await session.commit()
    await session.refresh(job)
    return job
