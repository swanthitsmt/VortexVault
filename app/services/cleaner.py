from __future__ import annotations

import glob
import os
import sqlite3
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import FileCheckpoint, ProcessingJob
from app.services.parser import format_combo_entry, parse_combo_line
from app.services.worker_state import (
    ensure_parent_dir,
    get_job,
    mark_completed,
    mark_failed,
    mark_paused,
    mark_running,
    refresh_meta,
    upsert_checkpoint,
)

ENCODING_CANDIDATES = ("utf-16", "utf-8", "latin-1")


def _detect_encoding(file_path: str) -> str:
    for encoding in ENCODING_CANDIDATES:
        try:
            with open(file_path, "r", encoding=encoding, errors="strict") as handle:
                handle.read(1024 * 64)
            return encoding
        except UnicodeError:
            continue
    return "latin-1"


def _open_dedupe_store(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("CREATE TABLE IF NOT EXISTS unique_entries (entry TEXT PRIMARY KEY)")
    conn.commit()
    return conn


def _is_pause_requested(session: Session, job_id: UUID) -> bool:
    stmt = select(ProcessingJob.pause_requested).where(ProcessingJob.id == job_id)
    value = session.execute(stmt).scalar_one_or_none()
    return bool(value)


def _resolve_source_files(source_path: str) -> list[str]:
    """
    Resolve cleaner inputs from:
    - directory: /path/to/dir (loads *.txt)
    - single file: /path/to/file.txt (or any readable file path)
    - glob: /path/to/*.txt
    """
    raw = source_path.strip()
    if not raw:
        return []

    expanded = os.path.expanduser(raw)

    # Explicit glob input.
    if any(token in expanded for token in ("*", "?", "[")):
        candidates = glob.glob(expanded)
        return sorted(str(Path(p)) for p in candidates if os.path.isfile(p))

    path = Path(expanded)
    if path.is_dir():
        return sorted(str(Path(p)) for p in glob.glob(str(path / "*.txt")) if os.path.isfile(p))
    if path.is_file():
        return [str(path)]
    return []


def run_clean_job(session: Session, job_id: UUID) -> None:
    job = get_job(session, job_id)
    if job is None:
        raise ValueError(f"Job {job_id} not found")

    if job.pause_requested:
        mark_paused(job)
        session.commit()
        return

    mark_running(job)
    session.commit()

    source_path = job.source_path
    files = _resolve_source_files(source_path)
    if not files:
        mark_failed(job, f"No input files found for source path: {source_path}")
        session.commit()
        return

    refresh_meta(job, total_files=len(files))

    job_dir = settings.job_workdir / str(job.id)
    dedupe_db_path = job_dir / "dedupe.sqlite3"

    checkpoint_count = session.execute(
        select(func.count(FileCheckpoint.id)).where(FileCheckpoint.job_id == job.id)
    ).scalar_one()

    if not job.output_file:
        output_path = job_dir / "cleaned_format.txt"
        job.output_file = str(output_path)
    else:
        output_path = Path(job.output_file)

    ensure_parent_dir(str(output_path))
    file_mode = "a" if checkpoint_count and output_path.exists() else "w"

    dedupe_conn = _open_dedupe_store(dedupe_db_path)
    dedupe_cursor = dedupe_conn.cursor()

    total_processed = int(job.processed_lines or 0)
    total_unique = int(job.unique_found or 0)

    session.commit()

    try:
        with open(output_path, file_mode, encoding="utf-8", buffering=1024 * 1024) as output_handle:
            for file_index, file_path in enumerate(files, start=1):
                checkpoint = session.execute(
                    select(FileCheckpoint).where(
                        FileCheckpoint.job_id == job.id,
                        FileCheckpoint.file_path == file_path,
                    )
                ).scalar_one_or_none()

                encoding = checkpoint.encoding if checkpoint and checkpoint.encoding else _detect_encoding(file_path)
                start_position = int(checkpoint.position) if checkpoint else 0
                file_processed_lines = int(checkpoint.processed_lines) if checkpoint else 0

                with open(file_path, "r", encoding=encoding, errors="ignore", buffering=1024 * 1024) as source_handle:
                    if start_position > 0:
                        source_handle.seek(start_position)

                    lines_since_flush = 0
                    while True:
                        raw_line = source_handle.readline()
                        if not raw_line:
                            break

                        file_processed_lines += 1
                        total_processed += 1
                        lines_since_flush += 1

                        parsed = parse_combo_line(raw_line)
                        if parsed is not None:
                            formatted = format_combo_entry(*parsed)
                            dedupe_cursor.execute(
                                "INSERT OR IGNORE INTO unique_entries(entry) VALUES (?)",
                                (formatted,),
                            )
                            if dedupe_cursor.rowcount == 1:
                                output_handle.write(formatted + "\n")
                                total_unique += 1

                        if lines_since_flush >= settings.cleaner_flush_every:
                            position = source_handle.tell()
                            dedupe_conn.commit()
                            output_handle.flush()

                            upsert_checkpoint(
                                session,
                                job_id=job.id,
                                file_path=file_path,
                                encoding=encoding,
                                position=position,
                                processed_lines=file_processed_lines,
                            )
                            job.current_file = file_path
                            job.current_line = file_processed_lines
                            job.processed_lines = total_processed
                            job.unique_found = total_unique
                            refresh_meta(
                                job,
                                file_index=file_index,
                                total_files=len(files),
                            )
                            session.commit()

                            if _is_pause_requested(session, job.id):
                                mark_paused(job)
                                session.commit()
                                return

                            lines_since_flush = 0

                    final_position = source_handle.tell()
                    dedupe_conn.commit()
                    output_handle.flush()

                upsert_checkpoint(
                    session,
                    job_id=job.id,
                    file_path=file_path,
                    encoding=encoding,
                    position=final_position,
                    processed_lines=file_processed_lines,
                )
                job.current_file = file_path
                job.current_line = file_processed_lines
                job.processed_lines = total_processed
                job.unique_found = total_unique
                refresh_meta(
                    job,
                    file_index=file_index,
                    total_files=len(files),
                )
                session.commit()

                if _is_pause_requested(session, job.id):
                    mark_paused(job)
                    session.commit()
                    return

        mark_completed(job)
        job.processed_lines = total_processed
        job.unique_found = total_unique
        session.commit()
    except Exception as exc:
        mark_failed(job, str(exc))
        session.commit()
        raise
    finally:
        dedupe_conn.close()
