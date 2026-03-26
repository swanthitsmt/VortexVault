from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import select
from sqlalchemy.orm import Session

from vortexvault.config import settings
from vortexvault.models import ExportJob, JobStatus
from vortexvault.services.meili import meili_router
from vortexvault.services.minio_store import minio_store


def run_export_job(session: Session, job_id: UUID) -> ExportJob:
    job = session.execute(select(ExportJob).where(ExportJob.id == job_id)).scalar_one()
    if job.status == JobStatus.completed:
        return job

    job.status = JobStatus.running
    if job.started_at is None:
        job.started_at = datetime.now(timezone.utc)
    session.commit()

    tmp_dir = Path(settings.export_tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    local_file = tmp_dir / f"{job.id}.parquet"

    schema = pa.schema(
        [
            ("url", pa.string()),
            ("username", pa.string()),
            ("password", pa.string()),
            ("score", pa.float64()),
            ("shard", pa.int16()),
        ]
    )

    loop = asyncio.new_event_loop()
    exported_rows = 0
    writer: pq.ParquetWriter | None = None

    try:
        writer = pq.ParquetWriter(local_file, schema=schema, compression="zstd")

        for shard_id in range(meili_router.shard_count):
            offset = 0
            while exported_rows < job.line_limit:
                page_limit = min(settings.export_page_size, job.line_limit - exported_rows)
                hits = loop.run_until_complete(
                    meili_router.search_shard(
                        shard_id=shard_id,
                        query=job.query_text,
                        limit=page_limit,
                        offset=offset,
                        filter_url=job.filter_url,
                        filter_username=job.filter_username,
                        prefix=True,
                        typo_tolerance=True,
                    )
                )
                if not hits:
                    break

                rows = [
                    {
                        "url": str(row.get("url", "")),
                        "username": str(row.get("username", "")),
                        "password": str(row.get("password", "")),
                        "score": float(row.get("score", 0.0) or 0.0),
                        "shard": int(row.get("shard", shard_id) or shard_id),
                    }
                    for row in hits
                ]
                table = pa.Table.from_pylist(rows, schema=schema)
                writer.write_table(table)

                page_count = len(rows)
                exported_rows += page_count
                offset += page_count
                job.exported_rows = exported_rows
                session.commit()

                if page_count < page_limit:
                    break

            if exported_rows >= job.line_limit:
                break

        object_key = f"exports/{job.id}.parquet"
        minio_store.client.upload_file(str(local_file), job.object_bucket, object_key)

        job.object_key = object_key
        job.status = JobStatus.completed
        job.finished_at = datetime.now(timezone.utc)
        session.commit()
        return job
    except Exception as exc:
        job.status = JobStatus.failed
        job.error_message = str(exc)
        job.finished_at = datetime.now(timezone.utc)
        session.commit()
        raise
    finally:
        if writer is not None:
            writer.close()
        loop.close()
        if local_file.exists():
            local_file.unlink()
