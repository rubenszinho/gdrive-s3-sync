
import json
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    
    google_service_account_json: str = Field(...)
    gdrive_folder: str = Field(default="fire-risk-predictor")
    gdrive_shared_with_me: bool = Field(default=True)

    
    s3_endpoint: str = Field(...)
    s3_access_key: str = Field(...)
    s3_secret_key: str = Field(...)
    s3_bucket: str = Field(default="fire-risk-predictor")
    s3_provider: str = Field(default="Minio")
    s3_region: Optional[str] = Field(default=None)

    
    sync_cron_schedule: str = Field(default="0 6,18 * * *")
    sync_transfers: int = Field(default=10)
    sync_checkers: int = Field(default=10)
    sync_retries: int = Field(default=5)
    sync_dry_run: bool = Field(default=False)
    sync_force_full: bool = Field(default=False)
    critical_files: str = Field(default="sisam_focos_2003.csv,RF.pkl,MLP.pkl,XGBoost.pkl")

    
    celery_broker_url: str = Field(default="redis://localhost:6379/0")
    celery_result_backend: str = Field(default="redis://localhost:6379/0")

    
    log_level: str = Field(default="INFO")
    rclone_config_path: Path = Field(default=Path("/tmp/rclone.conf"))
    service_account_path: Path = Field(default=Path("/tmp/service_account.json"))

    @field_validator("critical_files", mode="before")
    @classmethod
    def parse_critical_files(cls, v):
        if isinstance(v, list):
            return ",".join(v)
        return v

    @property
    def critical_files_list(self) -> list[str]:
        return [f.strip() for f in self.critical_files.split(",") if f.strip()]

    def get_service_account_dict(self) -> dict:
        import base64

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
            f"provider = {self.s3_provider}",
            f"access_key_id = {self.s3_access_key}",
            f"secret_access_key = {self.s3_secret_key}",
            f"endpoint = {self.s3_endpoint}",
            "acl = private",
        ]

        if self.s3_region:
            config_lines.append(f"region = {self.s3_region}")

        self.rclone_config_path.write_text("\n".join(config_lines))
        return self.rclone_config_path


settings = Settings()
