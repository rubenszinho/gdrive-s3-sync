
import json
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Google Drive (worker only)
    google_service_account_json: Optional[str] = Field(default=None)
    gdrive_folder: str = Field(...)

    # S3 (worker only)
    s3_endpoint: Optional[str] = Field(default=None)
    s3_access_key: Optional[str] = Field(default=None)
    s3_secret_key: Optional[str] = Field(default=None)
    s3_bucket: str = Field(...)
    s3_region: str = Field(default="us-east-1")

    # Sync
    sync_cron_schedule: str = Field(default="0 6,18 * * *")

    # Redis
    redis_url: str = Field(default="redis://localhost:6379/0")

    # Internal paths
    rclone_config_path: Path = Field(default=Path("/tmp/rclone.conf"))
    service_account_path: Path = Field(default=Path("/tmp/service_account.json"))

    def get_service_account_dict(self) -> dict:
        import base64

        if not self.google_service_account_json:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not configured")

        json_str = self.google_service_account_json
        try:
            decoded = base64.b64decode(json_str).decode("utf-8")
            return json.loads(decoded)
        except Exception:
            pass
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid service account JSON: {e}")

    def setup_rclone_config(self) -> Path:
        if not all([self.s3_endpoint, self.s3_access_key, self.s3_secret_key]):
            raise ValueError("S3 credentials not configured")

        sa_dict = self.get_service_account_dict()
        self.service_account_path.write_text(json.dumps(sa_dict, indent=2))

        config_lines = [
            "[gdrive]",
            "type = drive",
            "scope = drive.readonly",
            f"service_account_file = {self.service_account_path}",
            "",
            "[s3]",
            "type = s3",
            "provider = Other",
            f"access_key_id = {self.s3_access_key}",
            f"secret_access_key = {self.s3_secret_key}",
            f"endpoint = {self.s3_endpoint}",
            f"region = {self.s3_region}",
            "acl = private",
        ]

        self.rclone_config_path.write_text("\n".join(config_lines))
        return self.rclone_config_path


settings = Settings()
