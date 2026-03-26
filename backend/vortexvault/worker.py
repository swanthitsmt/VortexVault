from __future__ import annotations

import asyncio
from uuid import UUID

from celery import Task
from sqlalchemy import select

from vortexvault.celery_app import celery_app
from vortexvault.config import settings
from vortexvault.db import Base, SyncSessionLocal, sync_engine
from vortexvault.models import JobStatus, MergeJob
from vortexvault.services.export_pipeline import run_export_job
from vortexvault.services.ingest_pipeline import run_ingest_job
from vortexvault.services.meili import meili_router
from vortexvault.services.merge_pipeline import run_merge_job
from vortexvault.services.minio_store import minio_store

_INITIALIZED = False


class BaseTask(Task):
    autoretry_for = (Exception,)
    retry_backoff = True
    retry_jitter = True
    retry_kwargs = {"max_retries": 3}


def init_once() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return
    Base.metadata.create_all(bind=sync_engine)
    minio_store.ensure_bucket(settings.minio_bucket_raw)
    minio_store.ensure_bucket(settings.minio_bucket_export)
    asyncio.run(meili_router.ensure_indexes())
    _INITIALIZED = True


@celery_app.task(bind=True, base=BaseTask, name="vortexvault.worker.ingest_task", queue="ingest")
def ingest_task(self, job_id: str) -> None:
    init_once()
    with SyncSessionLocal() as session:
        job = run_ingest_job(session, UUID(job_id))

        auto_merge = bool((job.metadata_json or {}).get("auto_merge", True))
        if auto_merge and job.status == JobStatus.completed:
            existing = session.execute(select(MergeJob).where(MergeJob.ingest_job_id == job.id)).scalar_one_or_none()
            if existing is None:
                merge_job = MergeJob(
                    ingest_job_id=job.id,
                    status=JobStatus.queued,
                    notes="Auto-created after ingest completion",
                )
                session.add(merge_job)
                session.commit()
                session.refresh(merge_job)
                celery_app.send_task("vortexvault.worker.merge_task", args=[str(merge_job.id)], queue="merge")


@celery_app.task(bind=True, base=BaseTask, name="vortexvault.worker.merge_task", queue="merge")
def merge_task(self, merge_job_id: str) -> None:
    init_once()
    with SyncSessionLocal() as session:
        run_merge_job(session, UUID(merge_job_id))


@celery_app.task(bind=True, base=BaseTask, name="vortexvault.worker.export_task", queue="export")
def export_task(self, job_id: str) -> None:
    init_once()
    with SyncSessionLocal() as session:
        run_export_job(session, UUID(job_id))
