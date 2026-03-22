from __future__ import annotations

from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.models import JobStatus, JobType
from app.security import require_basic_auth
from app.services.jobs import create_job, get_job, list_jobs, request_pause, request_resume
from app.utils.serializers import serialize_job
from app.worker_tasks import clean_job_task, merge_job_task, pipeline_bundle_task, upload_job_task

router = APIRouter(prefix="/api", dependencies=[Depends(require_basic_auth)])


class CleanJobRequest(BaseModel):
    source_path: str
    output_file: str | None = None


class UploadJobRequest(BaseModel):
    source_path: str


class MergeJobRequest(BaseModel):
    source_path: str = "all_staging"


class PipelineJobRequest(BaseModel):
    source_path: str
    output_file: str | None = None


@router.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@router.get("/jobs")
async def jobs(session: AsyncSession = Depends(get_session)) -> dict:
    rows = await list_jobs(session, limit=200)
    return {"items": [serialize_job(row) for row in rows]}


@router.get("/jobs/{job_id}")
async def job_detail(job_id: UUID, session: AsyncSession = Depends(get_session)) -> dict:
    job = await get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return serialize_job(job)


@router.post("/jobs/clean")
async def api_start_clean(payload: CleanJobRequest, session: AsyncSession = Depends(get_session)) -> dict:
    job = await create_job(
        session,
        job_type=JobType.clean,
        source_path=payload.source_path.strip(),
        output_file=payload.output_file.strip() if payload.output_file else None,
    )

    task = clean_job_task.delay(str(job.id))
    job.celery_task_id = task.id
    await session.commit()

    return serialize_job(job)


@router.post("/jobs/upload")
async def api_start_upload(payload: UploadJobRequest, session: AsyncSession = Depends(get_session)) -> dict:
    job = await create_job(
        session,
        job_type=JobType.upload,
        source_path=payload.source_path.strip(),
    )

    task = upload_job_task.delay(str(job.id))
    job.celery_task_id = task.id
    await session.commit()

    return serialize_job(job)


@router.post("/jobs/merge")
async def api_start_merge(payload: MergeJobRequest, session: AsyncSession = Depends(get_session)) -> dict:
    job = await create_job(
        session,
        job_type=JobType.merge,
        source_path=payload.source_path.strip() or "all_staging",
    )

    task = merge_job_task.delay(str(job.id))
    job.celery_task_id = task.id
    await session.commit()

    return serialize_job(job)


@router.post("/jobs/pipeline")
async def api_start_pipeline(payload: PipelineJobRequest, session: AsyncSession = Depends(get_session)) -> dict:
    source = payload.source_path.strip()
    if not source:
        raise HTTPException(status_code=400, detail="source_path is required")

    bundle_id = str(uuid4())
    output_file = payload.output_file.strip() if payload.output_file else ""
    normalized_output = output_file or f"/app/data/output/pipeline_{bundle_id}.txt"

    clean_job = await create_job(
        session,
        job_type=JobType.clean,
        source_path=source,
        output_file=normalized_output,
        meta={
            "pipeline_bundle_id": bundle_id,
            "pipeline_role": "clean",
        },
    )

    upload_job = await create_job(
        session,
        job_type=JobType.upload,
        source_path="__pipeline_waiting_clean_output__",
        meta={
            "pipeline_bundle_id": bundle_id,
            "pipeline_role": "upload",
            "clean_job_id": str(clean_job.id),
        },
    )

    merge_job = await create_job(
        session,
        job_type=JobType.merge,
        source_path=f"job:{upload_job.id}",
        meta={
            "pipeline_bundle_id": bundle_id,
            "pipeline_role": "merge",
            "clean_job_id": str(clean_job.id),
            "upload_job_id": str(upload_job.id),
        },
    )

    clean_job.meta = {
        **(clean_job.meta or {}),
        "upload_job_id": str(upload_job.id),
        "merge_job_id": str(merge_job.id),
    }
    upload_job.meta = {
        **(upload_job.meta or {}),
        "merge_job_id": str(merge_job.id),
    }

    task = pipeline_bundle_task.delay(
        str(clean_job.id),
        str(upload_job.id),
        str(merge_job.id),
        bundle_id,
    )
    clean_job.celery_task_id = task.id
    upload_job.celery_task_id = task.id
    merge_job.celery_task_id = task.id
    await session.commit()

    return {
        "bundle_id": bundle_id,
        "task_id": task.id,
        "clean_job": serialize_job(clean_job),
        "upload_job": serialize_job(upload_job),
        "merge_job": serialize_job(merge_job),
    }


@router.post("/jobs/{job_id}/pause")
async def api_pause_job(job_id: UUID, session: AsyncSession = Depends(get_session)) -> dict:
    job = await get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    await request_pause(session, job)
    return serialize_job(job)


@router.post("/jobs/{job_id}/resume")
async def api_resume_job(job_id: UUID, session: AsyncSession = Depends(get_session)) -> dict:
    job = await get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in (JobStatus.paused, JobStatus.failed):
        return serialize_job(job)

    await request_resume(session, job)
    if job.job_type == JobType.clean:
        task = clean_job_task.delay(str(job.id))
    elif job.job_type == JobType.merge:
        task = merge_job_task.delay(str(job.id))
    else:
        task = upload_job_task.delay(str(job.id))
    job.celery_task_id = task.id
    await session.commit()

    return serialize_job(job)
