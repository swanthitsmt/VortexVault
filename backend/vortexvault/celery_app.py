from __future__ import annotations

from celery import Celery
from kombu import Queue

from vortexvault.config import settings

celery_app = Celery(
    "vortexvault_v2",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(
    task_track_started=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_default_queue="ingest",
    task_queues=(
        Queue("ingest"),
        Queue("merge"),
        Queue("export"),
    ),
    task_routes={
        "vortexvault.worker.ingest_task": {"queue": "ingest"},
        "vortexvault.worker.merge_task": {"queue": "merge"},
        "vortexvault.worker.export_task": {"queue": "export"},
    },
    task_time_limit=60 * 60 * 6,
    task_soft_time_limit=60 * 60 * 5,
    broker_connection_retry_on_startup=True,
    imports=("vortexvault.worker",),
)
