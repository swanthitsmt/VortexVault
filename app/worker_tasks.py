from __future__ import annotations

import logging
from threading import Lock
from uuid import UUID

from celery import Task

from app.celery_app import celery_app
from app.db import SyncSessionLocal, init_db_sync
from app.services.cleaner import run_clean_job
from app.services.pipeline import run_pipeline_bundle
from app.services.uploader import run_merge_job, run_upload_job

logger = logging.getLogger(__name__)
_db_init_lock = Lock()
_db_initialized = False


def _ensure_db_initialized() -> None:
    global _db_initialized
    if _db_initialized:
        return

    with _db_init_lock:
        if _db_initialized:
            return
        init_db_sync()
        _db_initialized = True


class BaseJobTask(Task):
    autoretry_for = (Exception,)
    retry_backoff = True
    retry_jitter = True
    retry_kwargs = {"max_retries": 3}


@celery_app.task(bind=True, base=BaseJobTask, name="app.tasks.clean_job", queue="clean")
def clean_job_task(self, job_id: str) -> None:
    _ensure_db_initialized()
    with SyncSessionLocal() as session:
        run_clean_job(session, UUID(job_id))


@celery_app.task(bind=True, base=BaseJobTask, name="app.tasks.upload_job", queue="upload")
def upload_job_task(self, job_id: str) -> None:
    _ensure_db_initialized()
    with SyncSessionLocal() as session:
        run_upload_job(session, UUID(job_id))


@celery_app.task(bind=True, base=BaseJobTask, name="app.tasks.merge_job", queue="merge")
def merge_job_task(self, job_id: str) -> None:
    _ensure_db_initialized()
    with SyncSessionLocal() as session:
        run_merge_job(session, UUID(job_id))


@celery_app.task(bind=True, base=BaseJobTask, name="app.tasks.pipeline_bundle", queue="pipeline")
def pipeline_bundle_task(self, clean_job_id: str, upload_job_id: str, merge_job_id: str, bundle_id: str) -> None:
    _ensure_db_initialized()
    with SyncSessionLocal() as session:
        run_pipeline_bundle(
            session,
            clean_job_id=UUID(clean_job_id),
            upload_job_id=UUID(upload_job_id),
            merge_job_id=UUID(merge_job_id),
            bundle_id=bundle_id,
        )
