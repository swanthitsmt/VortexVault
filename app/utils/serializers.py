from __future__ import annotations

from app.models import ProcessingJob


def serialize_job(job: ProcessingJob) -> dict:
    return {
        "id": str(job.id),
        "job_type": job.job_type.value,
        "status": job.status.value,
        "source_path": job.source_path,
        "output_file": job.output_file,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        "current_file": job.current_file,
        "current_line": int(job.current_line or 0),
        "processed_lines": int(job.processed_lines or 0),
        "unique_found": int(job.unique_found or 0),
        "rows_inserted": int(job.rows_inserted or 0),
        "rows_skipped": int(job.rows_skipped or 0),
        "pause_requested": job.pause_requested,
        "error_message": job.error_message,
        "meta": job.meta or {},
    }
