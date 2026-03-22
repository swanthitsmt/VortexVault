from __future__ import annotations

import logging
from pathlib import Path
import shutil
from uuid import UUID

from sqlalchemy import delete

from app.config import settings
from app.models import FileCheckpoint
from app.services.cleaner import run_clean_job
from app.services.uploader import run_merge_job, run_upload_job
from app.services.worker_state import get_job, mark_failed, refresh_meta

logger = logging.getLogger(__name__)


def _status_message(prefix: str, status: str, error_message: str | None) -> str:
    detail = f"{prefix} status={status}"
    if error_message:
        detail += f" error={error_message}"
    return detail


def _mark_failed_if_incomplete(session, job_id: UUID, message: str) -> None:
    job = get_job(session, job_id)
    if job is None:
        return
    if job.status.value in {"completed", "failed"}:
        return
    mark_failed(job, message)


def _cleanup_pipeline_artifacts(
    session,
    *,
    clean_job_id: UUID,
    upload_job_id: UUID,
    merge_job_id: UUID,
    cleaned_output: str,
) -> dict[str, object]:
    stats: dict[str, object] = {
        "output_deleted": False,
        "output_path": cleaned_output,
        "job_workdirs_deleted": [],
        "checkpoints_deleted": 0,
    }

    if settings.pipeline_cleanup_output_on_success and cleaned_output:
        output_path = Path(cleaned_output)
        try:
            if output_path.exists() and output_path.is_file():
                output_path.unlink()
                stats["output_deleted"] = True

                # Best-effort cleanup for empty nested dirs under /app/data/output.
                output_root = Path("/app/data/output")
                parent = output_path.parent
                while parent != output_root and output_root in parent.parents:
                    try:
                        parent.rmdir()
                    except OSError:
                        break
                    parent = parent.parent
        except Exception:
            logger.exception("Failed to delete pipeline output file: %s", cleaned_output)

    if settings.pipeline_cleanup_job_workdir_on_success:
        for job_id in (clean_job_id, upload_job_id, merge_job_id):
            job_dir = settings.job_workdir / str(job_id)
            try:
                if job_dir.exists():
                    shutil.rmtree(job_dir)
                    cast_list = stats["job_workdirs_deleted"]
                    if isinstance(cast_list, list):
                        cast_list.append(str(job_dir))
            except Exception:
                logger.exception("Failed to delete pipeline job workdir: %s", job_dir)

    if settings.pipeline_cleanup_checkpoints_on_success:
        deleted = session.execute(
            delete(FileCheckpoint).where(
                FileCheckpoint.job_id.in_([clean_job_id, upload_job_id, merge_job_id])
            )
        ).rowcount
        stats["checkpoints_deleted"] = int(deleted or 0)

    return stats


def run_pipeline_bundle(
    session,
    *,
    clean_job_id: UUID,
    upload_job_id: UUID,
    merge_job_id: UUID,
    bundle_id: str | None = None,
) -> None:
    """
    Execute clean -> upload -> merge sequentially for a pre-created job bundle.
    """
    clean_job = get_job(session, clean_job_id)
    upload_job = get_job(session, upload_job_id)
    merge_job = get_job(session, merge_job_id)
    if clean_job is None or upload_job is None or merge_job is None:
        raise ValueError("Pipeline bundle references missing job(s)")

    refresh_meta(clean_job, pipeline_bundle_id=bundle_id, pipeline_stage="clean")
    refresh_meta(upload_job, pipeline_bundle_id=bundle_id, pipeline_stage="upload")
    refresh_meta(merge_job, pipeline_bundle_id=bundle_id, pipeline_stage="merge")
    session.commit()

    try:
        run_clean_job(session, clean_job_id)
        clean_job = get_job(session, clean_job_id)
        if clean_job is None:
            raise RuntimeError("Cleaner job disappeared")
        if clean_job.status.value != "completed":
            raise RuntimeError(_status_message("Cleaner did not complete", clean_job.status.value, clean_job.error_message))

        cleaned_output = (clean_job.output_file or "").strip()
        if not cleaned_output:
            raise RuntimeError("Cleaner completed without output_file")
        if not Path(cleaned_output).exists():
            raise RuntimeError(f"Cleaner output file not found: {cleaned_output}")

        upload_job = get_job(session, upload_job_id)
        if upload_job is None:
            raise RuntimeError("Upload job disappeared")
        upload_job.source_path = cleaned_output
        refresh_meta(upload_job, pipeline_clean_job_id=str(clean_job_id), cleaned_output=cleaned_output)
        session.commit()

        run_upload_job(session, upload_job_id)
        upload_job = get_job(session, upload_job_id)
        if upload_job is None:
            raise RuntimeError("Upload job disappeared")
        if upload_job.status.value != "completed":
            raise RuntimeError(_status_message("Upload did not complete", upload_job.status.value, upload_job.error_message))

        merge_job = get_job(session, merge_job_id)
        if merge_job is None:
            raise RuntimeError("Merge job disappeared")
        merge_job.source_path = f"job:{upload_job_id}"
        refresh_meta(merge_job, pipeline_upload_job_id=str(upload_job_id))
        session.commit()

        run_merge_job(session, merge_job_id)
        merge_job = get_job(session, merge_job_id)
        if merge_job is None:
            raise RuntimeError("Merge job disappeared")
        if merge_job.status.value != "completed":
            raise RuntimeError(_status_message("Merge did not complete", merge_job.status.value, merge_job.error_message))

        cleanup_stats = _cleanup_pipeline_artifacts(
            session,
            clean_job_id=clean_job_id,
            upload_job_id=upload_job_id,
            merge_job_id=merge_job_id,
            cleaned_output=cleaned_output,
        )
        clean_job = get_job(session, clean_job_id)
        upload_job = get_job(session, upload_job_id)
        merge_job = get_job(session, merge_job_id)
        if clean_job is not None:
            refresh_meta(clean_job, pipeline_cleanup=cleanup_stats)
        if upload_job is not None:
            refresh_meta(upload_job, pipeline_cleanup=cleanup_stats)
        if merge_job is not None:
            refresh_meta(merge_job, pipeline_cleanup=cleanup_stats)
        session.commit()
    except Exception as exc:
        message = f"Pipeline bundle failed: {exc}"
        _mark_failed_if_incomplete(session, clean_job_id, message)
        _mark_failed_if_incomplete(session, upload_job_id, message)
        _mark_failed_if_incomplete(session, merge_job_id, message)
        session.commit()
        raise
