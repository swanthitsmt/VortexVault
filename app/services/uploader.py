from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import logging
import os
import re
from threading import Event
import time
from pathlib import Path
from uuid import UUID

import psycopg
from redis import Redis
from psycopg import sql
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.config import settings
from app.models import FileCheckpoint, JobStatus, JobType, ProcessingJob
from app.services.parser import parse_combo_line
from app.services.worker_state import (
    get_job,
    mark_completed,
    mark_failed,
    mark_paused,
    mark_running,
    refresh_meta,
    upsert_checkpoint,
)

logger = logging.getLogger(__name__)

STAGING_TABLE_PATTERN = re.compile(r"^staging_upload_[a-f0-9]{32}(?:_s[0-9]+)?$")
STAGING_TABLE_JOB_ID_PATTERN = re.compile(r"^staging_upload_([a-f0-9]{32})(?:_s[0-9]+)?$")

# ---------------------------------------------------------------------------
# GIN trigram index management for merge performance
# ---------------------------------------------------------------------------
_SEARCH_INDEXES = [
    {
        "name": "idx_combo_url_trgm",
        "create": "CREATE INDEX CONCURRENTLY idx_combo_url_trgm ON combo_entries USING gin (url gin_trgm_ops)",
        "drop": "DROP INDEX IF EXISTS idx_combo_url_trgm",
    },
    {
        "name": "idx_combo_username_trgm",
        "create": "CREATE INDEX CONCURRENTLY idx_combo_username_trgm ON combo_entries USING gin (username gin_trgm_ops)",
        "drop": "DROP INDEX IF EXISTS idx_combo_username_trgm",
    },
    {
        "name": "idx_combo_password_trgm",
        "create": "CREATE INDEX CONCURRENTLY idx_combo_password_trgm ON combo_entries USING gin (password gin_trgm_ops)",
        "drop": "DROP INDEX IF EXISTS idx_combo_password_trgm",
    },
    {
        "name": "idx_combo_created_at_desc",
        "create": "CREATE INDEX CONCURRENTLY idx_combo_created_at_desc ON combo_entries (created_at DESC)",
        "drop": "DROP INDEX IF EXISTS idx_combo_created_at_desc",
    },
]


def _drop_search_indexes(cur: psycopg.Cursor) -> None:
    """Drop GIN trigram and btree indexes to speed up bulk inserts."""
    for idx in _SEARCH_INDEXES:
        logger.info("Dropping index: %s", idx["name"])
        cur.execute(idx["drop"])


def _rebuild_search_indexes(dsn: str) -> None:
    """Rebuild indexes using CONCURRENTLY (requires autocommit connection)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            for idx in _SEARCH_INDEXES:
                cur.execute(
                    """
                    SELECT i.indisvalid
                    FROM pg_index i
                    JOIN pg_class c ON c.oid = i.indexrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public' AND c.relname = %s
                    """,
                    (idx["name"],),
                )
                row = cur.fetchone()
                exists = row is not None
                is_valid = bool(row[0]) if row else False

                if exists and is_valid:
                    logger.info("Index already valid: %s", idx["name"])
                    continue

                if exists and not is_valid:
                    logger.warning("Dropping invalid index before rebuild: %s", idx["name"])
                    cur.execute(
                        sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(
                            sql.Identifier(idx["name"])
                        )
                    )

                logger.info("Creating index: %s", idx["name"])
                cur.execute(idx["create"])


def _estimated_table_rows(cur: psycopg.Cursor, table_name: str) -> int:
    cur.execute(
        """
        SELECT COALESCE(GREATEST(reltuples, 0), 0)::BIGINT
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = %s
        """,
        (table_name,),
    )
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _is_pause_requested(session: Session, job_id: UUID) -> bool:
    stmt = select(ProcessingJob.pause_requested).where(ProcessingJob.id == job_id)
    value = session.execute(stmt).scalar_one_or_none()
    return bool(value)


def _compute_digest(url: str, username: str, password: str) -> bytes:
    payload = f"{url}\x1f{username}\x1f{password}".encode("utf-8", errors="ignore")
    return hashlib.sha256(payload).digest()


def _parse_cleaned_csv_line(raw_line: str) -> tuple[str, str, str] | None:
    line = raw_line.replace("\x00", "").strip()
    if not line or len(line) < 5:
        return None

    first = line.find(",")
    if first <= 0:
        return None
    second = line.find(",", first + 1)
    if second <= first + 1:
        return None

    url = line[:first].replace("\x00", "").strip()
    username = line[first + 1 : second].replace("\x00", "").strip()
    password = line[second + 1 :].replace("\x00", "").strip()
    if not url or not username or not password:
        return None
    return url, username, password


def _is_likely_cleaned_csv(source_file: Path, *, sample_limit: int = 256) -> bool:
    checked = 0
    csv_like = 0
    with open(source_file, "r", encoding="utf-8", errors="ignore", buffering=1024 * 1024) as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            checked += 1
            if _parse_cleaned_csv_line(line) is not None:
                csv_like += 1
            if checked >= sample_limit:
                break
    if checked == 0:
        return False
    return (csv_like / checked) >= 0.98


def _staging_table_for_job(job_id: UUID) -> str:
    return f"staging_upload_{job_id.hex}"


def _staging_tables_for_job(job_id: UUID, shard_count: int) -> list[str]:
    base = _staging_table_for_job(job_id)
    if shard_count <= 1:
        return [base]
    return [f"{base}_s{i:02d}" for i in range(shard_count)]


def _is_valid_staging_table_name(name: str) -> bool:
    return bool(STAGING_TABLE_PATTERN.match(name))


def _ensure_staging_table(cur: psycopg.Cursor, table_name: str) -> None:
    if not _is_valid_staging_table_name(table_name):
        raise ValueError(f"Invalid staging table name: {table_name}")

    cur.execute(
        sql.SQL(
            """
            CREATE UNLOGGED TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                source_job_id UUID NOT NULL,
                url TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                digest BYTEA NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        ).format(sql.Identifier(table_name))
    )


def _flush_upload_batch(
    cur: psycopg.Cursor,
    staging_table: str,
    batch: list[tuple[UUID, str, str, str, bytes]],
) -> int:
    if not batch:
        return 0

    copy_stmt = sql.SQL("COPY {} (source_job_id, url, username, password, digest) FROM STDIN").format(
        sql.Identifier(staging_table)
    )
    with cur.copy(copy_stmt) as copy:
        for source_job_id, url, username, password, digest in batch:
            copy.write_row(
                (
                    source_job_id,
                    url.replace("\x00", ""),
                    username.replace("\x00", ""),
                    password.replace("\x00", ""),
                    digest,
                )
            )
    return len(batch)


def _apply_upload_session_settings(cur: psycopg.Cursor) -> None:
    mode = (settings.upload_synchronous_commit or "off").strip().lower()
    allowed = {"on", "off", "local", "remote_write", "remote_apply"}
    if mode not in allowed:
        mode = "off"
    cur.execute(sql.SQL("SET synchronous_commit TO {}").format(sql.Identifier(mode)))


def _apply_merge_session_settings(cur: psycopg.Cursor) -> None:
    """Aggressive session tuning for merge bulk inserts."""
    cur.execute(sql.SQL("SET synchronous_commit TO {}").format(sql.Identifier("off")))
    cur.execute(
        sql.SQL("SET maintenance_work_mem TO {}").format(
            sql.Literal(settings.merge_maintenance_work_mem)
        )
    )
    cur.execute(
        sql.SQL("SET work_mem TO {}").format(
            sql.Literal(settings.merge_work_mem)
        )
    )


def _list_staging_tables(cur: psycopg.Cursor) -> list[str]:
    cur.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE 'staging_upload_%'
        ORDER BY tablename
        """
    )
    rows = cur.fetchall()
    return [str(row[0]) for row in rows if isinstance(row[0], str) and _is_valid_staging_table_name(str(row[0]))]


def _list_staging_tables_for_job(cur: psycopg.Cursor, job_id: UUID) -> list[str]:
    base = _staging_table_for_job(job_id)
    cur.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public' AND (tablename = %s OR tablename LIKE %s)
        ORDER BY tablename
        """,
        (base, f"{base}_s%"),
    )
    rows = cur.fetchall()
    tables = [str(row[0]) for row in rows if isinstance(row[0], str) and _is_valid_staging_table_name(str(row[0]))]
    return tables


def _extract_staging_job_id(table_name: str) -> UUID | None:
    match = STAGING_TABLE_JOB_ID_PATTERN.match(table_name)
    if not match:
        return None
    try:
        return UUID(hex=match.group(1))
    except ValueError:
        return None


def _cleanup_stale_staging_tables(session: Session, cur: psycopg.Cursor) -> dict[str, int]:
    if not bool(settings.staging_cleanup_enabled):
        return {"enabled": 0, "tables_scanned": 0, "tables_dropped": 0, "jobs_dropped": 0}

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(int(settings.staging_retention_hours), 1))
    all_tables = _list_staging_tables(cur)
    grouped: dict[UUID, list[str]] = {}
    for table_name in all_tables:
        job_id = _extract_staging_job_id(table_name)
        if job_id is not None:
            grouped.setdefault(job_id, []).append(table_name)

    dropped_tables = 0
    dropped_jobs = 0
    for upload_job_id, tables in grouped.items():
        row = session.execute(
            select(
                ProcessingJob.job_type,
                ProcessingJob.status,
                ProcessingJob.finished_at,
            ).where(ProcessingJob.id == upload_job_id)
        ).one_or_none()

        drop = False
        if row is None:
            drop = True
        else:
            job_type, status, finished_at = row
            is_upload = job_type == JobType.upload
            is_stale_failed = status in {JobStatus.failed, JobStatus.paused} and finished_at is not None and finished_at <= cutoff
            drop = is_upload and is_stale_failed

        if not drop:
            continue

        for table_name in tables:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))
            dropped_tables += 1
        dropped_jobs += 1

    return {
        "enabled": 1,
        "tables_scanned": len(all_tables),
        "tables_dropped": dropped_tables,
        "jobs_dropped": dropped_jobs,
    }


def _resolve_merge_sources(cur: psycopg.Cursor, source_tag: str) -> list[str]:
    raw = (source_tag or "").strip()
    lowered = raw.lower()

    if not raw or lowered in {"all", "all_staging", "staging_upload_*", "raw_ingest_entries"}:
        return _list_staging_tables(cur)

    if lowered.startswith("job:"):
        job_token = raw.split(":", 1)[1].strip()
        tables = _list_staging_tables_for_job(cur, UUID(job_token))
        if tables:
            return tables
        return [_staging_table_for_job(UUID(job_token))]

    tables = [part.strip() for part in raw.split(",") if part.strip()]
    if not tables:
        return _list_staging_tables(cur)

    invalid = [name for name in tables if not _is_valid_staging_table_name(name)]
    if invalid:
        joined = ", ".join(invalid)
        raise ValueError(f"Invalid staging source(s): {joined}")

    return tables


def _table_exists(cur: psycopg.Cursor, table_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
    row = cur.fetchone()
    return bool(row and row[0])


def _read_queue_depth(queue_name: str) -> int | None:
    try:
        client = Redis.from_url(settings.celery_broker_url, decode_responses=False)
        try:
            return int(client.llen(queue_name))
        finally:
            client.close()
    except Exception:
        return None


def _pg_active_session_count() -> int | None:
    try:
        with psycopg.connect(settings.postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::BIGINT
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND state IN ('active', 'idle in transaction')
                    """
                )
                row = cur.fetchone()
                return int(row[0] or 0) if row else None
    except Exception:
        return None


def _determine_upload_worker_count(
    session: Session,
    *,
    job_id: UUID,
    chunk_count: int,
) -> tuple[int, dict[str, int | bool | None]]:
    desired = max(int(settings.upload_manifest_parallel_workers), 1)
    cpu_limit = max((os.cpu_count() or 2) - 1, 1)

    running_uploads = int(
        session.execute(
            select(func.count(ProcessingJob.id)).where(
                ProcessingJob.job_type == JobType.upload,
                ProcessingJob.status == JobStatus.running,
                ProcessingJob.id != job_id,
            )
        ).scalar_one()
    )
    queue_lag = _read_queue_depth("upload")
    pg_active = _pg_active_session_count()

    worker_count = min(desired, chunk_count, cpu_limit)
    if settings.upload_auto_tune_workers:
        active_upload_count = running_uploads + 1
        fair_share = max(1, desired // active_upload_count)
        worker_count = min(worker_count, fair_share)

        if pg_active is not None and pg_active >= int(settings.upload_auto_tune_max_pg_active):
            worker_count = max(1, worker_count // 2)

        if queue_lag is not None and queue_lag >= 50 and running_uploads == 0:
            worker_count = min(desired, chunk_count, worker_count + 1)

    telemetry: dict[str, int | bool | None] = {
        "auto_tune_enabled": bool(settings.upload_auto_tune_workers),
        "configured_workers": desired,
        "cpu_limit": cpu_limit,
        "running_uploads": running_uploads,
        "queue_upload_lag": queue_lag,
        "pg_active_sessions": pg_active,
        "final_workers": worker_count,
    }
    return worker_count, telemetry


@dataclass(frozen=True)
class UploadChunk:
    index: int
    start: int
    end: int


def _build_line_safe_chunks(source_file: Path, target_bytes: int) -> list[UploadChunk]:
    size = int(source_file.stat().st_size)
    if size <= 0:
        return [UploadChunk(index=0, start=0, end=0)]

    chunks: list[UploadChunk] = []
    safe_target = max(int(target_bytes), 1024 * 1024)

    with open(source_file, "rb", buffering=8 * 1024 * 1024) as handle:
        start = 0
        chunk_index = 0
        while start < size:
            end = min(start + safe_target, size)
            if end < size:
                handle.seek(end)
                handle.readline()
                end = int(handle.tell())

            if end <= start:
                end = min(size, start + safe_target)

            chunks.append(UploadChunk(index=chunk_index, start=start, end=end))
            chunk_index += 1
            start = end

    return chunks


def _flush_all_chunk_batches(
    cur: psycopg.Cursor,
    batches: dict[str, list[tuple[UUID, str, str, str, bytes]]],
) -> int:
    inserted = 0
    for table_name, batch in batches.items():
        if not batch:
            continue
        inserted += _flush_upload_batch(cur, table_name, batch)
        batch.clear()
    return inserted


def _upload_manifest_chunk(
    *,
    source_file: Path,
    source_job_id: UUID,
    chunk: UploadChunk,
    staging_tables: list[str],
    shard_count: int,
    batch_size: int,
    commit_every_batches: int,
    fast_csv_mode: bool,
    stop_event: Event,
) -> dict[str, int]:
    processed_lines = 0
    rows_inserted = 0
    rows_skipped = 0
    pending_rows = 0
    pending_batches = 0

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            _apply_upload_session_settings(cur)
            batches: dict[str, list[tuple[UUID, str, str, str, bytes]]] = {
                table_name: [] for table_name in staging_tables
            }

            with open(source_file, "rb", buffering=8 * 1024 * 1024) as source_handle:
                source_handle.seek(chunk.start)

                while int(source_handle.tell()) < chunk.end:
                    if stop_event.is_set():
                        break

                    raw_line = source_handle.readline()
                    if not raw_line:
                        break

                    processed_lines += 1
                    decoded = raw_line.decode("utf-8", errors="ignore")
                    parsed = _parse_cleaned_csv_line(decoded) if fast_csv_mode else parse_combo_line(decoded)
                    if parsed is None:
                        rows_skipped += 1
                        continue

                    url, username, password = parsed
                    digest = _compute_digest(url, username, password)
                    shard_index = 0 if shard_count == 1 else (int.from_bytes(digest[:4], "big") % shard_count)
                    shard_table = staging_tables[shard_index]
                    batches[shard_table].append((source_job_id, url, username, password, digest))
                    pending_rows += 1

                    if pending_rows >= batch_size:
                        rows_inserted += _flush_all_chunk_batches(cur, batches)
                        pending_rows = 0
                        pending_batches += 1
                        if pending_batches >= commit_every_batches:
                            conn.commit()
                            pending_batches = 0

            rows_inserted += _flush_all_chunk_batches(cur, batches)
            conn.commit()

    return {
        "chunk_index": chunk.index,
        "processed_lines": processed_lines,
        "rows_inserted": rows_inserted,
        "rows_skipped": rows_skipped,
        "chunk_start": chunk.start,
        "chunk_end": chunk.end,
    }


def _run_upload_job_manifest_parallel(
    session: Session,
    *,
    job: ProcessingJob,
    source_file: Path,
    staging_table_base: str,
    staging_tables: list[str],
    shard_count: int,
    processed_lines: int,
    inserted_total: int,
    skipped_total: int,
    started_monotonic: float,
    fast_csv_mode: bool,
) -> tuple[int, int, int]:
    target_bytes = max(int(settings.upload_chunk_target_mb), 16) * 1024 * 1024
    chunks = _build_line_safe_chunks(source_file, target_bytes)
    worker_count, worker_telemetry = _determine_upload_worker_count(
        session,
        job_id=job.id,
        chunk_count=len(chunks),
    )
    worker_count = max(1, min(worker_count, len(chunks)))
    batch_size = max(settings.upload_batch_size, 1000)
    commit_every_batches = max(settings.upload_commit_every_batches, 1)
    stop_event = Event()

    refresh_meta(
        job,
        upload_mode="manifest_parallel",
        upload_manifest_chunk_count=len(chunks),
        upload_manifest_target_mb=max(int(settings.upload_chunk_target_mb), 16),
        upload_manifest_workers=worker_count,
        upload_worker_auto_tune=worker_telemetry,
        upload_manifest=[
            {"index": chunk.index, "start": chunk.start, "end": chunk.end}
            for chunk in chunks
        ],
        upload_chunks_completed=0,
        upload_chunk_progress_percent=0.0,
        staging_table=staging_table_base,
        staging_tables=staging_tables,
        upload_shard_count=shard_count,
    )
    session.commit()

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        future_to_chunk = {
            pool.submit(
                _upload_manifest_chunk,
                source_file=source_file,
                source_job_id=job.id,
                chunk=chunk,
                staging_tables=staging_tables,
                shard_count=shard_count,
                batch_size=batch_size,
                commit_every_batches=commit_every_batches,
                fast_csv_mode=fast_csv_mode,
                stop_event=stop_event,
            ): chunk
            for chunk in chunks
        }

        pending = set(future_to_chunk.keys())
        completed_chunks = 0
        while pending:
            done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)

            if _is_pause_requested(session, job.id):
                stop_event.set()
                for future in pending:
                    future.cancel()

            for future in done:
                if future.cancelled():
                    continue
                result = future.result()
                completed_chunks += 1

                processed_lines += int(result["processed_lines"])
                inserted_total += int(result["rows_inserted"])
                skipped_total += int(result["rows_skipped"])

                chunk_index = int(result["chunk_index"])
                chunk_processed = int(result["processed_lines"])
                chunk_end = int(result["chunk_end"])
                checkpoint_key = f"{source_file}::chunk:{chunk_index:05d}"

                upsert_checkpoint(
                    session,
                    job_id=job.id,
                    file_path=checkpoint_key,
                    encoding="utf-8",
                    position=chunk_end,
                    processed_lines=chunk_processed,
                )

                elapsed = max(time.monotonic() - started_monotonic, 0.001)
                progress_percent = round((completed_chunks / max(len(chunks), 1)) * 100.0, 2)
                rows_per_sec = inserted_total / elapsed
                refresh_meta(
                    job,
                    upload_chunks_completed=completed_chunks,
                    upload_chunk_progress_percent=progress_percent,
                    upload_rows_per_sec=round(rows_per_sec, 2),
                    upload_elapsed_sec=round(elapsed, 2),
                    staging_table=staging_table_base,
                    staging_tables=staging_tables,
                    upload_shard_count=shard_count,
                )
                job.current_file = f"{source_file.name} chunk {chunk_index + 1}/{len(chunks)}"
                job.current_line = chunk_processed
                job.processed_lines = processed_lines
                job.rows_inserted = inserted_total
                job.rows_skipped = skipped_total
                session.commit()

        if _is_pause_requested(session, job.id):
            mark_paused(job)
            session.commit()
            return processed_lines, inserted_total, skipped_total

    return processed_lines, inserted_total, skipped_total


def run_upload_job(session: Session, job_id: UUID) -> None:
    job = get_job(session, job_id)
    if job is None:
        raise ValueError(f"Job {job_id} not found")

    if job.pause_requested:
        mark_paused(job)
        session.commit()
        return

    mark_running(job)
    session.commit()

    source_file = Path(job.source_path)
    if not source_file.exists() or not source_file.is_file():
        mark_failed(job, f"Source cleaned file does not exist: {source_file}")
        session.commit()
        return

    checkpoint = session.execute(
        select(FileCheckpoint).where(
            FileCheckpoint.job_id == job.id,
            FileCheckpoint.file_path == str(source_file),
        )
    ).scalar_one_or_none()
    chunk_checkpoint_count = int(
        session.execute(
            select(func.count(FileCheckpoint.id)).where(
                FileCheckpoint.job_id == job.id,
                FileCheckpoint.file_path.like(f"{source_file}::chunk:%"),
            )
        ).scalar_one()
    )

    start_position = int(checkpoint.position) if checkpoint else 0
    processed_lines = int(checkpoint.processed_lines) if checkpoint else int(job.processed_lines or 0)
    inserted_total = int(job.rows_inserted or 0)
    skipped_total = int(job.rows_skipped or 0)

    if chunk_checkpoint_count > 0 and checkpoint is None:
        mark_failed(
            job,
            "Manifest-parallel upload resume is not supported for partial chunk checkpoints. "
            "Start a new upload job for this cleaned file.",
        )
        session.commit()
        return

    existing_meta = dict(job.meta or {})
    shard_count = max(int(settings.upload_shard_count), 1)
    staging_table_base = str(existing_meta.get("staging_table") or _staging_table_for_job(job.id))
    staging_tables = _staging_tables_for_job(job.id, shard_count)
    file_size_bytes = int(source_file.stat().st_size)
    manifest_threshold_bytes = max(int(settings.upload_manifest_min_file_mb), 1) * 1024 * 1024
    fast_csv_mode = str(existing_meta.get("upload_parser_mode", "")).strip().lower() == "cleaned_csv_fast"
    if not fast_csv_mode:
        try:
            # Cleaned output from cleaner is CSV-like `url,username,password` and can use a faster parser path.
            fast_csv_mode = _is_likely_cleaned_csv(source_file)
        except Exception:
            fast_csv_mode = False

    manifest_reasons: list[str] = []
    if not bool(settings.upload_manifest_enabled):
        manifest_reasons.append("manifest_disabled")
    if start_position != 0 or processed_lines != 0 or inserted_total != 0:
        manifest_reasons.append("resume_detected")
    if chunk_checkpoint_count != 0:
        manifest_reasons.append("partial_chunk_checkpoint_exists")
    if file_size_bytes < manifest_threshold_bytes:
        manifest_reasons.append("file_below_threshold")

    use_manifest_parallel = len(manifest_reasons) == 0

    refresh_meta(
        job,
        ingest_mode="staging",
        upload_mode="manifest_parallel" if use_manifest_parallel else "sequential",
        source_file=str(source_file),
        source_file_size_mb=round(file_size_bytes / (1024 * 1024), 2),
        upload_manifest_threshold_mb=max(int(settings.upload_manifest_min_file_mb), 1),
        upload_manifest_selected=use_manifest_parallel,
        upload_manifest_skip_reasons=manifest_reasons,
        upload_parser_mode="cleaned_csv_fast" if fast_csv_mode else "generic",
        staging_table=staging_table_base,
        staging_tables=staging_tables,
        upload_shard_count=shard_count,
    )
    session.commit()

    batch_size = max(settings.upload_batch_size, 1000)
    commit_every_batches = max(settings.upload_commit_every_batches, 1)
    started_monotonic = time.monotonic()

    try:
        with psycopg.connect(settings.postgres_dsn) as conn:
            with conn.cursor() as cur:
                _apply_upload_session_settings(cur)
                for table_name in staging_tables:
                    _ensure_staging_table(cur, table_name)
                conn.commit()

                if use_manifest_parallel:
                    processed_lines, inserted_total, skipped_total = _run_upload_job_manifest_parallel(
                        session,
                        job=job,
                        source_file=source_file,
                        staging_table_base=staging_table_base,
                        staging_tables=staging_tables,
                        shard_count=shard_count,
                        processed_lines=processed_lines,
                        inserted_total=inserted_total,
                        skipped_total=skipped_total,
                        started_monotonic=started_monotonic,
                        fast_csv_mode=fast_csv_mode,
                    )
                    upsert_checkpoint(
                        session,
                        job_id=job.id,
                        file_path=str(source_file),
                        encoding="utf-8",
                        position=file_size_bytes,
                        processed_lines=processed_lines,
                    )
                    if job.status.value == "paused":
                        return
                else:
                    with open(source_file, "r", encoding="utf-8", errors="ignore", buffering=1024 * 1024) as source_handle:
                        if start_position > 0:
                            source_handle.seek(start_position)

                        batches: dict[str, list[tuple[UUID, str, str, str, bytes]]] = {
                            table_name: [] for table_name in staging_tables
                        }
                        pending_batches = 0

                        def commit_and_checkpoint(position: int) -> None:
                            nonlocal pending_batches
                            if pending_batches == 0:
                                return

                            conn.commit()
                            pending_batches = 0

                            upsert_checkpoint(
                                session,
                                job_id=job.id,
                                file_path=str(source_file),
                                encoding="utf-8",
                                position=position,
                                processed_lines=processed_lines,
                            )

                            elapsed = max(time.monotonic() - started_monotonic, 0.001)
                            rows_per_sec = inserted_total / elapsed
                            refresh_meta(
                                job,
                                upload_rows_per_sec=round(rows_per_sec, 2),
                                upload_elapsed_sec=round(elapsed, 2),
                                staging_table=staging_table_base,
                                staging_tables=staging_tables,
                                upload_shard_count=shard_count,
                            )
                            job.current_file = str(source_file)
                            job.current_line = processed_lines
                            job.processed_lines = processed_lines
                            job.rows_inserted = inserted_total
                            job.rows_skipped = skipped_total
                            session.commit()

                        while True:
                            raw_line = source_handle.readline()
                            if not raw_line:
                                break

                            processed_lines += 1
                            parsed = _parse_cleaned_csv_line(raw_line) if fast_csv_mode else parse_combo_line(raw_line)
                            if parsed is None:
                                skipped_total += 1
                                continue

                            url, username, password = parsed
                            digest = _compute_digest(url, username, password)
                            shard_index = 0 if shard_count == 1 else (int.from_bytes(digest[:4], "big") % shard_count)
                            shard_table = staging_tables[shard_index]
                            batch = batches[shard_table]
                            batch.append((job.id, url, username, password, digest))

                            if len(batch) >= batch_size:
                                inserted = _flush_upload_batch(cur, shard_table, batch)
                                inserted_total += inserted
                                batch.clear()
                                pending_batches += 1

                                position = source_handle.tell()
                                if pending_batches >= commit_every_batches:
                                    commit_and_checkpoint(position)

                                if _is_pause_requested(session, job.id):
                                    commit_and_checkpoint(position)
                                    mark_paused(job)
                                    session.commit()
                                    return

                        for table_name, batch in batches.items():
                            if not batch:
                                continue
                            inserted = _flush_upload_batch(cur, table_name, batch)
                            inserted_total += inserted
                            batch.clear()
                            pending_batches += 1

                        final_position = source_handle.tell()
                        commit_and_checkpoint(final_position)
                        upsert_checkpoint(
                            session,
                            job_id=job.id,
                            file_path=str(source_file),
                            encoding="utf-8",
                            position=final_position,
                            processed_lines=processed_lines,
                        )

        job.current_file = str(source_file)
        job.current_line = processed_lines
        job.processed_lines = processed_lines
        job.rows_inserted = inserted_total
        job.rows_skipped = skipped_total
        elapsed = max(time.monotonic() - started_monotonic, 0.001)
        refresh_meta(
            job,
            upload_rows_per_sec=round(inserted_total / elapsed, 2),
            upload_elapsed_sec=round(elapsed, 2),
            staging_table=staging_table_base,
            staging_tables=staging_tables,
            upload_shard_count=shard_count,
            note="Upload completed into staging shard table(s). Run Merge job to move rows into combo_entries.",
        )
        mark_completed(job)
        session.commit()
    except Exception as exc:
        mark_failed(job, str(exc))
        session.commit()
        raise


def run_merge_job(session: Session, job_id: UUID) -> None:
    job = get_job(session, job_id)
    if job is None:
        raise ValueError(f"Job {job_id} not found")

    if job.pause_requested:
        mark_paused(job)
        session.commit()
        return

    mark_running(job)
    session.commit()

    batch_size = max(settings.merge_batch_size, 1000)
    commit_every_batches = max(settings.merge_commit_every_batches, 1)
    drop_on_success = bool(settings.merge_drop_staging_on_success)
    drop_indexes = bool(settings.merge_drop_indexes)
    started_monotonic = time.monotonic()

    inserted_before = int(job.rows_inserted or 0)
    inserted_total = inserted_before
    keys_inserted_total = 0
    processed_total = int(job.processed_lines or 0)
    dropped_tables: list[str] = []
    indexes_dropped = False
    merge_lock_key = int(settings.merge_advisory_lock_key)

    try:
        with psycopg.connect(settings.postgres_dsn) as conn:
            with conn.cursor() as cur:
                _apply_merge_session_settings(cur)
                cur.execute("SELECT pg_try_advisory_lock(%s)", (merge_lock_key,))
                lock_row = cur.fetchone()
                lock_acquired = bool(lock_row and lock_row[0])
                if not lock_acquired:
                    refresh_meta(job, merge_lock_key=merge_lock_key, merge_lock_acquired=False)
                    mark_failed(job, "Another merge job is already running (merge lock busy).")
                    session.commit()
                    return

                cleanup_stats = _cleanup_stale_staging_tables(session, cur)
                if int(cleanup_stats.get("tables_dropped", 0)) > 0:
                    conn.commit()

                source_tables = _resolve_merge_sources(cur, job.source_path)
                if not source_tables:
                    mark_failed(job, "No staging tables found for merge.")
                    session.commit()
                    return
                source_row_estimates = {table_name: _estimated_table_rows(cur, table_name) for table_name in source_tables}
                source_rows_estimated_total = sum(source_row_estimates.values())

                # --- Drop GIN indexes before bulk merge ---
                if drop_indexes:
                    logger.info("Dropping search indexes for merge performance...")
                    _drop_search_indexes(cur)
                    conn.commit()
                    indexes_dropped = True

                refresh_meta(
                    job,
                    merge_batch_size=batch_size,
                    merge_sources=source_tables,
                    merge_source_row_estimates=source_row_estimates,
                    merge_source_rows_estimated_total=source_rows_estimated_total,
                    merge_progress_percent=0.0,
                    merge_lock_key=merge_lock_key,
                    merge_lock_acquired=True,
                    indexes_dropped=indexes_dropped,
                    stale_staging_cleanup=cleanup_stats,
                )
                session.commit()

                pending_batches = 0

                def commit_merge_progress(table_name: str, last_staging_id: int, merged_from_table: int) -> None:
                    nonlocal pending_batches
                    if pending_batches > 0:
                        conn.commit()
                        pending_batches = 0

                    upsert_checkpoint(
                        session,
                        job_id=job.id,
                        file_path=table_name,
                        encoding=None,
                        position=last_staging_id,
                        processed_lines=merged_from_table,
                    )
                    elapsed = max(time.monotonic() - started_monotonic, 0.001)
                    progress_percent = None
                    if source_rows_estimated_total > 0:
                        progress_percent = round(
                            min((processed_total / source_rows_estimated_total) * 100.0, 100.0),
                            2,
                        )
                    refresh_meta(
                        job,
                        merge_rows_per_sec=round(inserted_total / elapsed, 2),
                        merge_elapsed_sec=round(elapsed, 2),
                        merge_current_table=table_name,
                        merge_last_staging_id=last_staging_id,
                        merge_processed_rows=processed_total,
                        merge_inserted_rows=inserted_total,
                        merge_keys_inserted=keys_inserted_total,
                        merge_progress_percent=progress_percent,
                    )
                    job.current_file = table_name
                    job.current_line = merged_from_table
                    job.processed_lines = processed_total
                    job.rows_inserted = inserted_total
                    session.commit()

                for table_name in source_tables:
                    if not _is_valid_staging_table_name(table_name):
                        raise ValueError(f"Invalid staging table: {table_name}")
                    if not _table_exists(cur, table_name):
                        raise ValueError(f"Staging table does not exist: {table_name}")

                    checkpoint = session.execute(
                        select(FileCheckpoint).where(
                            FileCheckpoint.job_id == job.id,
                            FileCheckpoint.file_path == table_name,
                        )
                    ).scalar_one_or_none()
                    last_staging_id = int(checkpoint.position) if checkpoint else 0
                    merged_from_table = int(checkpoint.processed_lines) if checkpoint else 0

                    while True:
                        if _is_pause_requested(session, job.id):
                            commit_merge_progress(table_name, last_staging_id, merged_from_table)
                            mark_paused(job)
                            session.commit()
                            return

                        merge_stmt = sql.SQL(
                            """
                            WITH chunk AS (
                                SELECT id, url, username, password, digest, created_at
                                FROM {}
                                WHERE id > %s
                                ORDER BY id
                                LIMIT %s
                            ),
                            chunk_dedup AS (
                                SELECT DISTINCT ON (digest)
                                    url, username, password, digest, created_at
                                FROM chunk
                                ORDER BY digest, id DESC
                            ),
                            ins_keys AS (
                                INSERT INTO combo_keys (digest)
                                SELECT c.digest
                                FROM chunk_dedup c
                                ON CONFLICT (digest) DO NOTHING
                                RETURNING digest
                            ),
                            ins AS (
                                INSERT INTO combo_entries (url, username, password, digest, created_at)
                                SELECT c.url, c.username, c.password, c.digest, c.created_at
                                FROM chunk_dedup c
                                JOIN ins_keys k ON k.digest = c.digest
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM combo_entries ce
                                    WHERE ce.digest = c.digest
                                )
                                RETURNING 1
                            )
                            SELECT
                                COALESCE((SELECT MAX(id) FROM chunk), 0)::BIGINT AS max_id,
                                COALESCE((SELECT COUNT(*) FROM chunk), 0)::BIGINT AS chunk_rows,
                                COALESCE((SELECT COUNT(*) FROM ins), 0)::BIGINT AS inserted_rows,
                                COALESCE((SELECT COUNT(*) FROM ins_keys), 0)::BIGINT AS inserted_keys
                            """
                        ).format(sql.Identifier(table_name))
                        cur.execute(merge_stmt, (last_staging_id, batch_size))
                        row = cur.fetchone()
                        max_id = int(row[0] or 0) if row else 0
                        chunk_rows = int(row[1] or 0) if row else 0
                        inserted_rows = int(row[2] or 0) if row else 0
                        inserted_keys = int(row[3] or 0) if row else 0
                        skipped_rows = chunk_rows - inserted_rows

                        if chunk_rows == 0:
                            break

                        last_staging_id = max_id
                        merged_from_table += inserted_rows
                        processed_total += chunk_rows
                        inserted_total += inserted_rows
                        keys_inserted_total += inserted_keys
                        job.rows_skipped = int(job.rows_skipped or 0) + skipped_rows
                        pending_batches += 1

                        if pending_batches >= commit_every_batches:
                            commit_merge_progress(table_name, last_staging_id, merged_from_table)

                    commit_merge_progress(table_name, last_staging_id, merged_from_table)

                    if drop_on_success:
                        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))
                        conn.commit()
                        dropped_tables.append(table_name)

                cur.execute("ANALYZE combo_entries")
                conn.commit()

        elapsed = max(time.monotonic() - started_monotonic, 0.001)
        refresh_meta(
            job,
            merge_rows_per_sec=round(inserted_total / elapsed, 2),
            merge_elapsed_sec=round(elapsed, 2),
            merge_processed_rows=processed_total,
            merge_inserted_rows=inserted_total,
            merge_keys_inserted=keys_inserted_total,
            merge_progress_percent=100.0 if processed_total > 0 else 0.0,
            dropped_staging_tables=dropped_tables,
        )
        job.processed_lines = processed_total
        job.rows_inserted = inserted_total
        mark_completed(job)
        session.commit()
    except Exception as exc:
        mark_failed(job, str(exc))
        session.commit()
        raise
    finally:
        # Always rebuild indexes — even on failure or pause
        if indexes_dropped:
            logger.info("Rebuilding search indexes after merge...")
            try:
                _rebuild_search_indexes(settings.postgres_dsn)
                logger.info("Search indexes rebuilt successfully.")
            except Exception:
                logger.exception("Failed to rebuild search indexes! Manual rebuild required.")

        # Sync newly merged rows to Elasticsearch
        sync_rows = max(inserted_total - inserted_before, 0)
        if settings.es_enabled and sync_rows > 0:
            _sync_to_elasticsearch(sync_rows)


def _sync_to_elasticsearch(row_count: int) -> None:
    """Sync recently inserted rows from combo_entries to Elasticsearch."""
    import asyncio

    async def _run_sync():
        from app.services.es import bulk_index_rows, ensure_index

        await ensure_index()

        batch_size = settings.es_sync_batch_size
        synced = 0

        with psycopg.connect(settings.postgres_dsn) as conn:
            with conn.cursor() as cur:
                # Get the max id to determine the sync range
                cur.execute("SELECT MAX(id) FROM combo_entries")
                max_id_row = cur.fetchone()
                max_id = int(max_id_row[0] or 0) if max_id_row else 0

                if max_id == 0:
                    return

                # Sync from (max_id - row_count) to max_id in batches
                start_id = max(0, max_id - row_count)
                current_id = start_id

                while current_id < max_id:
                    cur.execute(
                        "SELECT id, url, username, password, created_at "
                        "FROM combo_entries WHERE id > %s ORDER BY id LIMIT %s",
                        (current_id, batch_size),
                    )
                    rows = cur.fetchall()
                    if not rows:
                        break

                    indexed = await bulk_index_rows(rows)
                    synced += indexed
                    current_id = rows[-1][0]

        logger.info("ES sync complete: %d documents indexed", synced)

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_run_sync())
        else:
            asyncio.run(_run_sync())
    except RuntimeError:
        asyncio.run(_run_sync())
    except Exception:
        logger.exception("Elasticsearch sync failed (non-fatal)")
