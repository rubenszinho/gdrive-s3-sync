"""FastAPI app for health checks and manual sync triggers."""

from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel

import sys
sys.path.insert(0, "/app")

from shared import settings
from celery import Celery

celery_app = Celery(
    "sync_service",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

app = FastAPI(
    title="GDrive-S3 Sync API",
    version="1.0.0",
)


class SyncRequest(BaseModel):
    dry_run: bool = False
    force_full: bool = False


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


@app.get("/")
async def root():
    return {
        "service": "GDrive-S3 Sync API",
        "gdrive_folder": settings.gdrive_folder,
        "s3_bucket": settings.s3_bucket,
        "sync_schedule": settings.sync_cron_schedule,
    }


@app.get("/health")
async def get_health():
    try:
        celery_inspect = celery_app.control.inspect()
        active_workers = celery_inspect.active()
        celery_status = "connected" if active_workers else "no_workers"
    except Exception as e:
        celery_status = f"error: {e}"

    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "celery_status": celery_status,
        "configuration": {
            "gdrive_folder": settings.gdrive_folder,
            "s3_bucket": settings.s3_bucket,
            "sync_schedule": settings.sync_cron_schedule,
        },
    }


@app.post("/sync", response_model=TaskResponse)
async def trigger_sync(request: SyncRequest):
    task = celery_app.send_task(
        "tasks.sync_gdrive_to_s3",
        kwargs={"dry_run": request.dry_run, "force_full": request.force_full},
    )
    mode = "dry_run" if request.dry_run else ("force_full" if request.force_full else "incremental")
    return TaskResponse(task_id=task.id, status="queued", message=f"Sync queued in {mode} mode")


@app.post("/health-check", response_model=TaskResponse)
async def trigger_health_check():
    task = celery_app.send_task("tasks.health_check")
    return TaskResponse(task_id=task.id, status="queued", message="Health check queued")


@app.get("/task/{task_id}")
async def get_task_status(task_id: str):
    result = celery_app.AsyncResult(task_id)
    response = {"task_id": task_id, "status": result.status}

    if result.ready():
        response["result"] = result.result if result.successful() else str(result.result)
    elif result.status == "PROGRESS":
        response["progress"] = result.info

    return response


@app.get("/schedule")
async def get_schedule():
    return {
        "cron_expression": settings.sync_cron_schedule,
        "timezone": "UTC",
    }
