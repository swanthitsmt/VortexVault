from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from vortexvault.config import settings
from vortexvault.models import IngestJob, JobStatus
from vortexvault.services.dedupe import dedupe_service
from vortexvault.services.meili import meili_router
from vortexvault.services.minio_store import minio_store
from vortexvault.services.parser import parse_chunk_lines, parse_combo_line


def _digest(url: str, username: str, password: str) -> str:
    payload = f"{url}\x1f{username}\x1f{password}".encode("utf-8", errors="ignore")
    return hashlib.sha256(payload).hexdigest()


def _load_job(session: Session, job_id: UUID) -> IngestJob:
    return session.execute(select(IngestJob).where(IngestJob.id == job_id)).scalar_one()


def _checkpoint_progress(job: IngestJob, byte_offset: int, started: float, shard_counts: dict[str, int]) -> None:
    elapsed = max(time.perf_counter() - started, 0.001)
    rows_per_sec = round(job.processed_lines / elapsed, 2)
    mb_per_sec = round((byte_offset / (1024 * 1024)) / elapsed, 2)
    job.checkpoint_offset = byte_offset
    job.last_checkpoint_at = datetime.now(timezone.utc)
    job.shard_counts = shard_counts
    job.metadata_json = {
        **(job.metadata_json or {}),
        "rows_per_sec": rows_per_sec,
        "mb_per_sec": mb_per_sec,
        "checkpoint_offset": byte_offset,
    }


def run_ingest_job(session: Session, job_id: UUID) -> IngestJob:
    job = _load_job(session, job_id)
    if job.status == JobStatus.completed:
        return job

    dedupe_service.ensure_filter()
    started = time.perf_counter()

    job.status = JobStatus.running
    if job.started_at is None:
        job.started_at = datetime.now(timezone.utc)

    source_size = minio_store.stat_object(job.source_bucket, job.source_object)
    job.source_size_bytes = source_size
    session.commit()

    start_offset = int(job.checkpoint_offset or 0)
    if start_offset >= source_size and source_size > 0:
        job.status = JobStatus.completed
        job.finished_at = datetime.now(timezone.utc)
        session.commit()
        return job

    checkpoint_stride = settings.checkpoint_stride_bytes
    next_checkpoint = start_offset + checkpoint_stride
    chunk_size = settings.ingest_stream_chunk_bytes

    shard_counts: dict[str, int] = {str(i): int(job.shard_counts.get(str(i), 0)) for i in range(meili_router.shard_count)}
    batch_by_shard: dict[int, list[dict]] = {idx: [] for idx in range(meili_router.shard_count)}

    loop = asyncio.new_event_loop()

    def flush(shard_ids: list[int] | None = None, force: bool = False) -> None:
        ids = shard_ids if shard_ids is not None else list(batch_by_shard.keys())
        tasks = []
        for shard_id in ids:
            docs = batch_by_shard[shard_id]
            if not docs:
                continue
            if len(docs) < settings.ingest_batch_docs and not force:
                continue
            payload = docs[:]
            docs.clear()
            tasks.append(meili_router.index_documents(shard_id, payload))
        if tasks:
            loop.run_until_complete(asyncio.gather(*tasks))

    body = None
    try:
        kwargs = {"Bucket": job.source_bucket, "Key": job.source_object}
        if start_offset > 0:
            kwargs["Range"] = f"bytes={start_offset}-"
        response = minio_store.client.get_object(**kwargs)
        body = response["Body"]

        carry = b""
        byte_offset = start_offset

        while True:
            chunk = body.read(chunk_size)
            if not chunk:
                break

            byte_offset += len(chunk)
            parsed_rows, carry, invalid_count = parse_chunk_lines(carry + chunk)
            job.invalid_lines += invalid_count

            for url, username, password in parsed_rows:
                job.processed_lines += 1
                digest_hex = _digest(url, username, password)
                if not dedupe_service.is_new(digest_hex):
                    job.duplicate_lines += 1
                    continue

                shard_id = meili_router.shard_for_digest(digest_hex)
                shard_counts[str(shard_id)] = shard_counts.get(str(shard_id), 0) + 1
                batch_by_shard[shard_id].append(
                    {
                        "id": f"{digest_hex}:{job.id}:{job.processed_lines}",
                        "url": url,
                        "username": username,
                        "password": password,
                        "digest": digest_hex,
                        "shard": shard_id,
                        "ingested_at": int(time.time()),
                    }
                )
                job.indexed_docs += 1

                if len(batch_by_shard[shard_id]) >= settings.ingest_batch_docs:
                    flush([shard_id], force=True)

            if byte_offset >= next_checkpoint:
                flush(force=True)
                _checkpoint_progress(job, byte_offset, started, shard_counts)
                session.commit()
                next_checkpoint += checkpoint_stride

        if carry:
            parsed = parse_combo_line(carry)
            if parsed is None:
                job.invalid_lines += 1
            else:
                job.processed_lines += 1
                url, username, password = parsed
                digest_hex = _digest(url, username, password)
                if dedupe_service.is_new(digest_hex):
                    shard_id = meili_router.shard_for_digest(digest_hex)
                    shard_counts[str(shard_id)] = shard_counts.get(str(shard_id), 0) + 1
                    batch_by_shard[shard_id].append(
                        {
                            "id": f"{digest_hex}:{job.id}:{job.processed_lines}",
                            "url": url,
                            "username": username,
                            "password": password,
                            "digest": digest_hex,
                            "shard": shard_id,
                            "ingested_at": int(time.time()),
                        }
                    )
                    job.indexed_docs += 1
                else:
                    job.duplicate_lines += 1

        flush(force=True)
        _checkpoint_progress(job, byte_offset, started, shard_counts)
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
        if body is not None:
            body.close()
        loop.close()
