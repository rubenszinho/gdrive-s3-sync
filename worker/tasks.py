
import structlog
from datetime import datetime

import sys
sys.path.insert(0, "/app")

from celery_app import app
from shared import sync_service, settings

logger = structlog.get_logger()


@app.task(bind=True, name="tasks.sync_gdrive_to_s3")
def sync_gdrive_to_s3(self, dry_run: bool = False, force_full: bool = False) -> dict:
    log = logger.bind(task_id=self.request.id)
    log.info("Starting sync task", dry_run=dry_run, force_full=force_full)

    self.update_state(state="PROGRESS", meta={"status": "initializing"})

    try:
        sync_service.setup()

        self.update_state(state="PROGRESS", meta={"status": "testing_connections"})
        connections = sync_service.test_connections()

        if not connections["gdrive"]:
            raise RuntimeError("Failed to connect to Google Drive")
        if not connections["s3"]:
            raise RuntimeError("Failed to connect to S3")

        self.update_state(state="PROGRESS", meta={"status": "syncing"})
        result = sync_service.sync(dry_run=dry_run, force_full=force_full)

        result_dict = result.to_dict()
        if result.success:
            log.info("Sync completed successfully")
        else:
            log.error("Sync completed with errors", errors=result.errors)

        return result_dict

    except Exception as e:
        log.exception("Sync task failed", error=str(e))
        raise


@app.task(name="tasks.health_check")
def health_check() -> dict:
    log = logger.bind(task_name="health_check")

    try:
        sync_service.setup()
        connections = sync_service.test_connections()

        source_files, source_size = sync_service.get_remote_size(
            f"gdrive:{settings.gdrive_folder}/",
            drive_shared=True,
        )
        dest_files, dest_size = sync_service.get_remote_size(f"s3:{settings.s3_bucket}")

        return {
            "status": "healthy" if all(connections.values()) else "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "connections": connections,
            "source": {"folder": settings.gdrive_folder, "files": source_files},
            "destination": {"bucket": settings.s3_bucket, "files": dest_files},
        }
    except Exception as e:
        log.exception("Health check failed")
        return {"status": "error", "timestamp": datetime.utcnow().isoformat(), "error": str(e)}
