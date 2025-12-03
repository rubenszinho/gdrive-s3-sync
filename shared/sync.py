
import subprocess
import json
import structlog
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from .config import settings

logger = structlog.get_logger()


@dataclass
class SyncResult:

    success: bool
    started_at: datetime
    finished_at: datetime
    mode: str
    source_files: int = 0
    source_size_bytes: int = 0
    destination_files: int = 0
    destination_size_bytes: int = 0
    errors: list[str] = field(default_factory=list)
    log_output: str = ""

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "mode": self.mode,
            "source_files": self.source_files,
            "source_size_mb": round(self.source_size_bytes / (1024**2), 2),
            "destination_files": self.destination_files,
            "destination_size_mb": round(self.destination_size_bytes / (1024**2), 2),
            "errors": self.errors,
        }


class SyncService:

    def __init__(self):
        self.config_path: Optional[Path] = None
        self.log = logger.bind(service="sync")

    def _run_rclone(
        self,
        args: list[str],
        capture_output: bool = True,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        cmd = ["rclone", f"--config={self.config_path}"] + args
        self.log.debug("Running rclone command", cmd=" ".join(cmd))

        try:
            return subprocess.run(cmd, capture_output=capture_output, text=True, check=check)
        except subprocess.CalledProcessError as e:
            self.log.error("Rclone command failed", cmd=" ".join(cmd), returncode=e.returncode)
            raise

    def _get_gdrive_source(self) -> str:
        return f"gdrive:{settings.gdrive_folder}/"

    def _get_s3_destination(self) -> str:
        return f"s3:{settings.s3_bucket}"

    def setup(self) -> None:
        self.log.info("Setting up rclone configuration")
        self.config_path = settings.setup_rclone_config()

    def test_connections(self) -> dict[str, bool]:
        results = {"gdrive": False, "s3": False}

        try:
            args = ["lsf", self._get_gdrive_source(), "--max-depth", "1", "--drive-shared-with-me"]
            self._run_rclone(args)
            results["gdrive"] = True
        except Exception as e:
            self.log.error("Google Drive connection failed", error=str(e))

        try:
            self._run_rclone(["lsf", self._get_s3_destination(), "--max-depth", "1"], check=False)
            results["s3"] = True
        except Exception as e:
            self.log.error("S3 connection failed", error=str(e))

        return results

    def get_remote_size(self, remote_path: str, drive_shared: bool = False) -> tuple[int, int]:
        args = ["size", remote_path, "--json"]
        if drive_shared and remote_path.startswith("gdrive:"):
            args.append("--drive-shared-with-me")

        try:
            result = self._run_rclone(args, check=False)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return data.get("count", 0), data.get("bytes", 0)
        except Exception as e:
            self.log.warning("Failed to get remote size", error=str(e))

        return 0, 0

    def purge_destination(self) -> None:
        self.log.warning("Purging S3 bucket", bucket=settings.s3_bucket)
        self._run_rclone(["delete", self._get_s3_destination(), "--rmdirs", "-v"])

    def sync(
        self,
        dry_run: bool = False,
        force_full: bool = False,
    ) -> SyncResult:
        started_at = datetime.utcnow()
        errors = []
        mode = "dry_run" if dry_run else ("force_full" if force_full else "incremental")

        self.log.info("Starting sync", mode=mode)

        if not self.config_path:
            self.setup()

        source_files, source_size = self.get_remote_size(self._get_gdrive_source(), drive_shared=True)

        if force_full and not dry_run:
            try:
                self.purge_destination()
            except Exception as e:
                errors.append(f"Purge failed: {e}")

        args = [
            "sync",
            self._get_gdrive_source(),
            self._get_s3_destination(),
            "--drive-shared-with-me",
            "--update",
            "--use-server-modtime",
            "--transfers=10",
            "--checkers=10",
            "--retries=5",
            "--low-level-retries=15",
            "--multi-thread-streams=4",
            "--s3-upload-concurrency=10",
            "--s3-chunk-size=25M",
            "--stats=30s",
            "-v",
        ]

        if dry_run:
            args.append("--dry-run")

        log_output = ""
        success = False

        try:
            result = self._run_rclone(args, check=False)
            log_output = result.stdout + result.stderr
            success = result.returncode == 0
            if not success:
                errors.append(f"Sync failed with exit code {result.returncode}")
        except Exception as e:
            errors.append(f"Sync exception: {e}")

        dest_files, dest_size = self.get_remote_size(self._get_s3_destination())

        return SyncResult(
            success=success,
            started_at=started_at,
            finished_at=datetime.utcnow(),
            mode=mode,
            source_files=source_files,
            source_size_bytes=source_size,
            destination_files=dest_files,
            destination_size_bytes=dest_size,
            errors=errors,
            log_output=log_output,
        )


sync_service = SyncService()
