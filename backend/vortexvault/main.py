from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from vortexvault.celery_app import celery_app
from vortexvault.config import settings
from vortexvault.db import Base, async_engine, get_async_session
from vortexvault.models import ExportJob, IngestJob, JobStatus, MergeJob, SearchMetric, UploadSession, UploadStatus
from vortexvault.schemas import (
    DashboardResponse,
    ExportCreateRequest,
    ExportJobResponse,
    IngestCreateRequest,
    IngestJobResponse,
    MergeCreateRequest,
    MergeJobResponse,
    MultipartCompleteRequest,
    MultipartInitRequest,
    MultipartInitResponse,
    MultipartPartRequest,
    MultipartPartResponse,
    PresignUploadRequest,
    PresignUploadResponse,
    SearchHit,
    SearchQueryRequest,
    SearchQueryResponse,
)
from vortexvault.security import auth_middleware, sanitize_bucket_name, sanitize_object_name, validate_runtime_security_or_raise
from vortexvault.services.dedupe import dedupe_service
from vortexvault.services.meili import meili_router
from vortexvault.services.minio_store import minio_store

app = FastAPI(title=settings.app_name, version="2.0.0")
app.middleware("http")(auth_middleware)

if settings.cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["Authorization", "Content-Type"],
    )


@app.on_event("startup")
async def startup() -> None:
    validate_runtime_security_or_raise()
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    minio_store.ensure_bucket(settings.minio_bucket_raw)
    minio_store.ensure_bucket(settings.minio_bucket_export)
    await meili_router.ensure_indexes()


@app.get("/")
async def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.post("/api/v2/files/presign", response_model=PresignUploadResponse)
async def presign_upload(payload: PresignUploadRequest) -> PresignUploadResponse:
    safe_object_name = sanitize_object_name(payload.object_name)
    object_key = f"raw/{safe_object_name}"
    put_url = minio_store.presign_put(settings.minio_bucket_raw, object_key)
    return PresignUploadResponse(bucket=settings.minio_bucket_raw, object_key=object_key, put_url=put_url)


@app.post("/api/v2/files/multipart/init", response_model=MultipartInitResponse)
async def multipart_init(payload: MultipartInitRequest, session: AsyncSession = Depends(get_async_session)) -> MultipartInitResponse:
    safe_object_name = sanitize_object_name(payload.object_name)
    object_key = f"raw/{safe_object_name}"
    upload_id = minio_store.initiate_multipart_upload(settings.minio_bucket_raw, object_key)

    upload_session = UploadSession(
        bucket=settings.minio_bucket_raw,
        object_key=object_key,
        upload_id=upload_id,
        total_parts=payload.total_parts,
        status=UploadStatus.initiated,
    )
    session.add(upload_session)
    await session.commit()
    await session.refresh(upload_session)

    return MultipartInitResponse(
        session_id=upload_session.id,
        bucket=upload_session.bucket,
        object_key=upload_session.object_key,
        upload_id=upload_session.upload_id,
    )


@app.post("/api/v2/files/multipart/part", response_model=MultipartPartResponse)
async def multipart_part(payload: MultipartPartRequest, session: AsyncSession = Depends(get_async_session)) -> MultipartPartResponse:
    upload_session = await session.get(UploadSession, payload.session_id)
    if not upload_session:
        raise HTTPException(status_code=404, detail="Upload session not found")
    if upload_session.status != UploadStatus.initiated:
        raise HTTPException(status_code=409, detail="Upload session is not active")

    url = minio_store.presign_upload_part(
        upload_session.bucket,
        upload_session.object_key,
        upload_session.upload_id,
        payload.part_number,
    )
    return MultipartPartResponse(part_number=payload.part_number, presigned_url=url)


@app.post("/api/v2/files/multipart/complete")
async def multipart_complete(payload: MultipartCompleteRequest, session: AsyncSession = Depends(get_async_session)) -> dict[str, str]:
    upload_session = await session.get(UploadSession, payload.session_id)
    if not upload_session:
        raise HTTPException(status_code=404, detail="Upload session not found")
    if upload_session.status != UploadStatus.initiated:
        raise HTTPException(status_code=409, detail="Upload session is not active")

    part_numbers = [part.part_number for part in payload.parts]
    if not part_numbers or len(part_numbers) != len(set(part_numbers)):
        raise HTTPException(status_code=422, detail="Multipart parts must be unique")
    if len(part_numbers) > upload_session.total_parts:
        raise HTTPException(status_code=422, detail="Multipart part count exceeds initialized total parts")

    try:
        minio_store.complete_multipart_upload(
            upload_session.bucket,
            upload_session.object_key,
            upload_session.upload_id,
            [{"ETag": part.etag, "PartNumber": part.part_number} for part in payload.parts],
        )
    except Exception as exc:
        minio_store.abort_multipart_upload(upload_session.bucket, upload_session.object_key, upload_session.upload_id)
        raise HTTPException(status_code=400, detail=f"Multipart complete failed: {exc}") from exc

    upload_session.status = UploadStatus.completed
    upload_session.completed_at = datetime.now(timezone.utc)
    await session.commit()
    return {"status": "completed", "object_key": upload_session.object_key}


@app.post("/api/v2/ingest/jobs", response_model=IngestJobResponse)
async def create_ingest_job(payload: IngestCreateRequest, session: AsyncSession = Depends(get_async_session)) -> IngestJobResponse:
    safe_bucket = sanitize_bucket_name(payload.source_bucket)
    safe_object = sanitize_object_name(payload.source_object)
    job = IngestJob(
        status=JobStatus.queued,
        source_bucket=safe_bucket,
        source_object=safe_object,
        metadata_json={"auto_merge": payload.auto_merge},
        shard_counts={str(i): 0 for i in range(settings.shard_count)},
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)

    celery_app.send_task("vortexvault.worker.ingest_task", args=[str(job.id)], queue="ingest")
    return IngestJobResponse.model_validate(job, from_attributes=True)


@app.get("/api/v2/ingest/jobs/{job_id}", response_model=IngestJobResponse)
async def get_ingest_job(job_id: UUID, session: AsyncSession = Depends(get_async_session)) -> IngestJobResponse:
    job = await session.get(IngestJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Ingest job not found")
    return IngestJobResponse.model_validate(job, from_attributes=True)


@app.post("/api/v2/ingest/jobs/{job_id}/resume", response_model=IngestJobResponse)
async def resume_ingest_job(job_id: UUID, session: AsyncSession = Depends(get_async_session)) -> IngestJobResponse:
    job = await session.get(IngestJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Ingest job not found")
    if job.status == JobStatus.running:
        return IngestJobResponse.model_validate(job, from_attributes=True)

    job.status = JobStatus.queued
    await session.commit()
    celery_app.send_task("vortexvault.worker.ingest_task", args=[str(job.id)], queue="ingest")
    await session.refresh(job)
    return IngestJobResponse.model_validate(job, from_attributes=True)


@app.post("/api/v2/merge/jobs", response_model=MergeJobResponse)
async def create_merge_job(payload: MergeCreateRequest, session: AsyncSession = Depends(get_async_session)) -> MergeJobResponse:
    ingest_job = await session.get(IngestJob, payload.ingest_job_id)
    if not ingest_job:
        raise HTTPException(status_code=404, detail="Ingest job not found")

    existing = (await session.execute(select(MergeJob).where(MergeJob.ingest_job_id == payload.ingest_job_id))).scalar_one_or_none()
    if existing:
        return MergeJobResponse.model_validate(existing, from_attributes=True)

    merge_job = MergeJob(ingest_job_id=payload.ingest_job_id, status=JobStatus.queued)
    session.add(merge_job)
    await session.commit()
    await session.refresh(merge_job)

    celery_app.send_task("vortexvault.worker.merge_task", args=[str(merge_job.id)], queue="merge")
    return MergeJobResponse.model_validate(merge_job, from_attributes=True)


@app.get("/api/v2/merge/jobs/{merge_job_id}", response_model=MergeJobResponse)
async def get_merge_job(merge_job_id: UUID, session: AsyncSession = Depends(get_async_session)) -> MergeJobResponse:
    merge_job = await session.get(MergeJob, merge_job_id)
    if not merge_job:
        raise HTTPException(status_code=404, detail="Merge job not found")
    return MergeJobResponse.model_validate(merge_job, from_attributes=True)


@app.post("/api/v2/search/query", response_model=SearchQueryResponse)
async def search_query(payload: SearchQueryRequest, session: AsyncSession = Depends(get_async_session)) -> SearchQueryResponse:
    limit = min(payload.limit, settings.search_max_limit)
    hits, took_ms = await meili_router.federated_search(
        query=payload.query,
        limit=limit,
        filter_url=payload.filter_url,
        filter_username=payload.filter_username,
        prefix=payload.prefix,
        typo_tolerance=payload.typo_tolerance,
    )

    mapped = [
        SearchHit(
            id=str(row.get("id", "")),
            url=str(row.get("url", "")),
            username=str(row.get("username", "")),
            password=str(row.get("password", "")),
            score=float(row.get("score", 0.0) or 0.0),
            shard=int(row.get("shard", 0) or 0),
        )
        for row in hits
    ]

    query_hash = hashlib.sha256(payload.query.encode("utf-8")).hexdigest()
    metric = SearchMetric(query_hash=query_hash, latency_ms=int(took_ms), result_count=len(mapped))
    session.add(metric)
    await session.commit()

    return SearchQueryResponse(took_ms=round(took_ms, 3), total_hits=len(mapped), hits=mapped)


@app.post("/api/v2/exports", response_model=ExportJobResponse)
async def create_export_job(payload: ExportCreateRequest, session: AsyncSession = Depends(get_async_session)) -> ExportJobResponse:
    job = ExportJob(
        status=JobStatus.queued,
        query_text=payload.query,
        filter_url=payload.filter_url,
        filter_username=payload.filter_username,
        line_limit=payload.line_limit,
        object_bucket=settings.minio_bucket_export,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)

    celery_app.send_task("vortexvault.worker.export_task", args=[str(job.id)], queue="export")
    return ExportJobResponse.model_validate(job, from_attributes=True)


@app.get("/api/v2/exports/{job_id}", response_model=ExportJobResponse)
async def get_export_job(job_id: UUID, session: AsyncSession = Depends(get_async_session)) -> ExportJobResponse:
    job = await session.get(ExportJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")
    return ExportJobResponse.model_validate(job, from_attributes=True)


@app.get("/api/v2/exports/{job_id}/download")
async def download_export(job_id: UUID, session: AsyncSession = Depends(get_async_session)) -> dict[str, str]:
    job = await session.get(ExportJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")
    if job.status != JobStatus.completed or not job.object_key:
        raise HTTPException(status_code=409, detail="Export not ready")

    url = minio_store.presign_get(job.object_bucket, job.object_key, expires_sec=settings.export_presign_ttl_sec)
    return {"download_url": url}


@app.get("/api/v2/dashboard", response_model=DashboardResponse)
async def dashboard(session: AsyncSession = Depends(get_async_session)) -> DashboardResponse:
    active_ingest = (
        await session.execute(select(func.count()).select_from(IngestJob).where(IngestJob.status.in_([JobStatus.queued, JobStatus.running])))
    ).scalar_one()
    active_merge = (
        await session.execute(select(func.count()).select_from(MergeJob).where(MergeJob.status.in_([JobStatus.queued, JobStatus.running])))
    ).scalar_one()
    active_export = (
        await session.execute(select(func.count()).select_from(ExportJob).where(ExportJob.status.in_([JobStatus.queued, JobStatus.running])))
    ).scalar_one()
    completed_ingest = (
        await session.execute(select(func.count()).select_from(IngestJob).where(IngestJob.status == JobStatus.completed))
    ).scalar_one()

    return DashboardResponse(
        active_ingest_jobs=int(active_ingest or 0),
        active_merge_jobs=int(active_merge or 0),
        active_export_jobs=int(active_export or 0),
        total_completed_ingest=int(completed_ingest or 0),
        dedupe_cardinality_estimate=dedupe_service.cardinality_estimate(),
        shard_count=settings.shard_count,
    )
