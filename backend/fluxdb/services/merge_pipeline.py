from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from fluxdb.models import IngestJob, JobStatus, MergeJob
from fluxdb.services.dedupe import dedupe_service
from fluxdb.services.minio_store import minio_store


def run_merge_job(session: Session, merge_job_id: UUID) -> MergeJob:
    merge_job = session.execute(select(MergeJob).where(MergeJob.id == merge_job_id)).scalar_one()
    ingest_job = session.execute(select(IngestJob).where(IngestJob.id == merge_job.ingest_job_id)).scalar_one()

    if merge_job.status == JobStatus.completed:
        return merge_job

    merge_job.status = JobStatus.running
    if merge_job.started_at is None:
        merge_job.started_at = datetime.now(timezone.utc)
    session.commit()

    try:
        estimate = dedupe_service.cardinality_estimate()
        merge_job.bloom_cardinality_estimate = estimate

        # Clean transient upload chunks if any worker stored temporary objects.
        cleanup_prefix = f"tmp/{ingest_job.id}/"
        cleaned_objects = minio_store.delete_prefix(ingest_job.source_bucket, cleanup_prefix)
        merge_job.cleaned_objects = cleaned_objects

        ingest_job.metadata_json = {
            **(ingest_job.metadata_json or {}),
            "dedupe_cardinality_estimate": estimate,
            "merge_cleaned_objects": cleaned_objects,
        }

        merge_job.status = JobStatus.completed
        merge_job.finished_at = datetime.now(timezone.utc)
        session.commit()
        return merge_job
    except Exception as exc:
        merge_job.status = JobStatus.failed
        merge_job.error_message = str(exc)
        merge_job.finished_at = datetime.now(timezone.utc)
        session.commit()
        raise
