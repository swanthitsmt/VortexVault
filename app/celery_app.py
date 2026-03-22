from __future__ import annotations

from celery import Celery
from kombu import Queue

from app.config import settings

celery_app = Celery(
    "massive_combo_cleaner",
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
    task_default_queue="pipeline",
    task_queues=(
        Queue("clean"),
        Queue("upload"),
        Queue("merge"),
        Queue("pipeline"),
    ),
    task_routes={
        "app.tasks.clean_job": {"queue": "clean"},
        "app.tasks.upload_job": {"queue": "upload"},
        "app.tasks.merge_job": {"queue": "merge"},
        "app.tasks.pipeline_bundle": {"queue": "pipeline"},
    },
    imports=("app.worker_tasks",),
)
