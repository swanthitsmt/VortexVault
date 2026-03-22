from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, status
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import AsyncSessionLocal, get_session
from app.models import JobStatus, JobType
from app.security import require_basic_auth
from app.services.dashboard import load_dashboard_metrics
from app.services.jobs import create_job, get_job, list_jobs, request_pause, request_resume
from app.services.search import run_search, stream_csv
from app.utils.serializers import serialize_job
from app.worker_tasks import clean_job_task, merge_job_task, pipeline_bundle_task, upload_job_task

BASE_DIR = Path(__file__).resolve().parents[1]
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

router = APIRouter(dependencies=[Depends(require_basic_auth)])


def _redirect(path: str) -> RedirectResponse:
    return RedirectResponse(path, status_code=status.HTTP_303_SEE_OTHER)


def _validate_regex_inputs(*values: str) -> None:
    for value in values:
        if value:
            re.compile(value)


def _validate_regex_term_lengths(*values: str) -> None:
    minimum = settings.regex_min_chars
    for value in values:
        if value and len(value.strip()) < minimum:
            raise ValueError(f"Regex filters must be at least {minimum} characters.")


def _has_any_filter(*values: str) -> bool:
    return any(value.strip() for value in values)


@router.get("/")
async def root_redirect():
    return _redirect("/dashboard")


@router.get("/dashboard")
async def dashboard(request: Request, session: AsyncSession = Depends(get_session)):
    metrics = await load_dashboard_metrics(session)
    recent_jobs = await list_jobs(session, limit=10)
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "metrics": metrics,
            "recent_jobs": recent_jobs,
        },
    )


@router.get("/pipeline")
async def pipeline_page(request: Request, session: AsyncSession = Depends(get_session)):
    jobs = await list_jobs(session, limit=50)
    return templates.TemplateResponse(
        "pipeline.html",
        {
            "request": request,
            "jobs": jobs,
            "job_status": JobStatus,
        },
    )


@router.post("/jobs/clean")
async def start_clean_job(
    source_path: str = Form(...),
    output_file: str = Form(""),
    session: AsyncSession = Depends(get_session),
):
    normalized_output = output_file.strip() or None

    job = await create_job(
        session,
        job_type=JobType.clean,
        source_path=source_path.strip(),
        output_file=normalized_output,
    )

    async_result = clean_job_task.delay(str(job.id))
    job.celery_task_id = async_result.id
    await session.commit()

    return _redirect(f"/jobs/{job.id}")


@router.post("/jobs/pipeline")
async def start_pipeline_bundle(
    source_path: str = Form(...),
    output_file: str = Form(""),
    session: AsyncSession = Depends(get_session),
):
    source = source_path.strip()
    if not source:
        raise HTTPException(status_code=400, detail="source_path is required")

    bundle_id = str(uuid4())
    normalized_output = output_file.strip() or f"/app/data/output/pipeline_{bundle_id}.txt"

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

    async_result = pipeline_bundle_task.delay(
        str(clean_job.id),
        str(upload_job.id),
        str(merge_job.id),
        bundle_id,
    )
    clean_job.celery_task_id = async_result.id
    upload_job.celery_task_id = async_result.id
    merge_job.celery_task_id = async_result.id
    await session.commit()

    return _redirect(f"/jobs/{clean_job.id}")


@router.post("/jobs/upload")
async def start_upload_job(
    cleaned_file: str = Form(...),
    session: AsyncSession = Depends(get_session),
):
    job = await create_job(
        session,
        job_type=JobType.upload,
        source_path=cleaned_file.strip(),
    )

    async_result = upload_job_task.delay(str(job.id))
    job.celery_task_id = async_result.id
    await session.commit()

    return _redirect(f"/jobs/{job.id}")


@router.post("/jobs/merge")
async def start_merge_job(
    source_tag: str = Form("all_staging"),
    session: AsyncSession = Depends(get_session),
):
    job = await create_job(
        session,
        job_type=JobType.merge,
        source_path=(source_tag.strip() or "all_staging"),
    )

    async_result = merge_job_task.delay(str(job.id))
    job.celery_task_id = async_result.id
    await session.commit()

    return _redirect(f"/jobs/{job.id}")


@router.get("/jobs/{job_id}")
async def job_detail(
    request: Request,
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    job = await get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return templates.TemplateResponse(
        "job_detail.html",
        {
            "request": request,
            "job": job,
            "job_json": serialize_job(job),
        },
    )


@router.post("/jobs/{job_id}/pause")
async def pause_job(job_id: UUID, session: AsyncSession = Depends(get_session)):
    job = await get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    await request_pause(session, job)
    return _redirect(f"/jobs/{job_id}")


@router.post("/jobs/{job_id}/resume")
async def resume_job(job_id: UUID, session: AsyncSession = Depends(get_session)):
    job = await get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in (JobStatus.paused, JobStatus.failed):
        return _redirect(f"/jobs/{job_id}")

    await request_resume(session, job)

    if job.job_type == JobType.clean:
        async_result = clean_job_task.delay(str(job.id))
    elif job.job_type == JobType.merge:
        async_result = merge_job_task.delay(str(job.id))
    else:
        async_result = upload_job_task.delay(str(job.id))

    job.celery_task_id = async_result.id
    await session.commit()

    return _redirect(f"/jobs/{job_id}")


@router.get("/search")
async def search_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
    url: str = Query(default=""),
    username: str = Query(default=""),
    password: str = Query(default=""),
    regex: bool = Query(default=False),
    include_total: bool = Query(default=False),
    cursor: int | None = Query(default=None, ge=1),
    direction: str = Query(default="next"),
    page_size: int = Query(default=25),
):
    search_notice = ""
    if page_size not in {10, 25, 50, 100}:
        page_size = 25

    direction = direction.lower().strip()
    if direction not in {"next", "prev"}:
        direction = "next"
    if cursor is None:
        direction = "next"

    if not _has_any_filter(url, username, password):
        results = {
            "total": 0,
            "page_size": page_size,
            "execution_ms": 0.0,
            "rows": [],
            "cursor": None,
            "direction": "next",
            "has_next": False,
            "has_prev": False,
            "next_cursor": None,
            "prev_cursor": None,
            "first_id": None,
            "last_id": None,
        }
        query_base = urlencode(
            {
                "url": url,
                "username": username,
                "password": password,
                "regex": "true" if regex else "false",
                "include_total": "true" if include_total else "false",
                "page_size": page_size,
            }
        )
        return templates.TemplateResponse(
            "search.html",
            {
                "request": request,
                "results": results,
                "url": url,
                "username": username,
                "password": password,
                "regex": regex,
                "include_total": include_total,
                "page_size": page_size,
                "query_base": query_base,
                "search_error": "Please add at least one filter before searching.",
                "search_notice": search_notice,
            },
        )

    search_error = ""
    if regex:
        try:
            _validate_regex_inputs(url, username, password)
            _validate_regex_term_lengths(url, username, password)
        except re.error:
            search_error = "Invalid regex pattern. Please check your expression and try again."
            results = {
                "total": 0,
                "page_size": page_size,
                "execution_ms": 0.0,
                "rows": [],
                "cursor": None,
                "direction": "next",
                "has_next": False,
                "has_prev": False,
                "next_cursor": None,
                "prev_cursor": None,
                "first_id": None,
                "last_id": None,
            }
            query_base = urlencode(
                {
                    "url": url,
                    "username": username,
                    "password": password,
                    "regex": "true" if regex else "false",
                    "include_total": "true" if include_total else "false",
                    "page_size": page_size,
                }
            )
            return templates.TemplateResponse(
                "search.html",
                {
                    "request": request,
                    "results": results,
                    "url": url,
                    "username": username,
                    "password": password,
                    "regex": regex,
                    "include_total": include_total,
                    "page_size": page_size,
                    "query_base": query_base,
                    "search_error": search_error,
                    "search_notice": search_notice,
                },
            )
        except ValueError as exc:
            search_error = str(exc)
            results = {
                "total": 0,
                "page_size": page_size,
                "execution_ms": 0.0,
                "rows": [],
                "cursor": None,
                "direction": "next",
                "has_next": False,
                "has_prev": False,
                "next_cursor": None,
                "prev_cursor": None,
                "first_id": None,
                "last_id": None,
            }
            query_base = urlencode(
                {
                    "url": url,
                    "username": username,
                    "password": password,
                    "regex": "true" if regex else "false",
                    "include_total": "true" if include_total else "false",
                    "page_size": page_size,
                }
            )
            return templates.TemplateResponse(
                "search.html",
                {
                    "request": request,
                    "results": results,
                    "url": url,
                    "username": username,
                    "password": password,
                    "regex": regex,
                    "include_total": include_total,
                    "page_size": page_size,
                    "query_base": query_base,
                    "search_error": search_error,
                    "search_notice": search_notice,
                },
            )

    try:
        results = await run_search(
            session,
            url=url or None,
            username=username or None,
            password=password or None,
            regex=regex,
            page_size=page_size,
            cursor=cursor,
            direction=direction,
            include_total=include_total,
            statement_timeout_ms=settings.search_statement_timeout_ms,
            count_statement_timeout_ms=settings.search_count_timeout_ms,
        )
    except DBAPIError as exc:
        message = str(getattr(exc, "orig", exc)).lower()
        if regex and "regular expression" in message:
            search_error = "Invalid regex pattern. Please check your expression and try again."
        elif "statement timeout" in message:
            search_error = "Search timed out. Narrow filters or increase SEARCH_STATEMENT_TIMEOUT_MS / SEARCH_COUNT_TIMEOUT_MS."
        else:
            search_error = "Search query failed. Please review filters and try again."

        results = {
            "total": None,
            "page_size": page_size,
            "execution_ms": 0.0,
            "rows": [],
            "cursor": cursor,
            "direction": direction,
            "has_next": False,
            "has_prev": cursor is not None,
            "next_cursor": None,
            "prev_cursor": None,
            "first_id": None,
            "last_id": None,
        }

    if results.get("count_timed_out"):
        search_notice = "Exact total count timed out, so page results are shown without total."

    query_base = urlencode(
        {
            "url": url,
            "username": username,
            "password": password,
            "regex": "true" if regex else "false",
            "include_total": "true" if include_total else "false",
            "page_size": page_size,
        }
    )

    return templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "results": results,
            "url": url,
            "username": username,
            "password": password,
            "regex": regex,
            "include_total": include_total,
            "page_size": page_size,
            "query_base": query_base,
            "search_error": search_error,
            "search_notice": search_notice,
        },
    )


@router.get("/search/export")
async def export_search_csv(
    url: str = Query(default=""),
    username: str = Query(default=""),
    password: str = Query(default=""),
    regex: bool = Query(default=False),
):
    if not _has_any_filter(url, username, password):
        raise HTTPException(status_code=400, detail="Add at least one filter before export.")

    if regex:
        try:
            _validate_regex_inputs(url, username, password)
            _validate_regex_term_lengths(url, username, password)
        except re.error:
            raise HTTPException(status_code=400, detail="Invalid regex pattern.")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    generator = stream_csv(
        AsyncSessionLocal,
        url=url or None,
        username=username or None,
        password=password or None,
        regex=regex,
        max_rows=settings.max_export_rows,
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    headers = {"Content-Disposition": f'attachment; filename="fluxdb_search_export_{ts}.csv"'}
    return StreamingResponse(generator, media_type="text/csv", headers=headers)
