"""Celery application configuration."""

from celery import Celery
from celery.schedules import crontab

import sys
sys.path.insert(0, "/app")

from shared import settings

app = Celery(
    "sync_service",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["tasks"],
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=7200,
    task_soft_time_limit=6900,
    worker_prefetch_multiplier=1,
    worker_concurrency=1,
    result_expires=86400 * 7,
)


def parse_cron_schedule(cron_expr: str) -> crontab:
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: {cron_expr}")

    minute, hour, day_of_month, month, day_of_week = parts
    return crontab(
        minute=minute,
        hour=hour,
        day_of_month=day_of_month,
        month_of_year=month,
        day_of_week=day_of_week,
    )


app.conf.beat_schedule = {
    "sync-gdrive-to-s3": {
        "task": "tasks.sync_gdrive_to_s3",
        "schedule": parse_cron_schedule(settings.sync_cron_schedule),
        "options": {"queue": "sync", "expires": 3600},
    },
    "health-check": {
        "task": "tasks.health_check",
        "schedule": crontab(minute="*/15"),
        "options": {"queue": "sync", "expires": 60},
    },
}
