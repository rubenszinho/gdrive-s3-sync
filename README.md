# GDrive-S3 Sync Service

Syncs Google Drive folders to S3-compatible storage using Celery Beat.

## Structure

```
sync-service/
├── shared/          # Shared config and sync logic
├── worker/          # Celery worker + beat scheduler
├── api/             # FastAPI for triggers and health
└── docker-compose.yml
```

## Quick Start

```bash
cd sync-service
cp .env.example .env   # Edit with your credentials
docker compose up -d
```

## Required Environment Variables

| Variable                      | Description                               |
| ----------------------------- | ----------------------------------------- |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Service account JSON (raw or base64)      |
| `GDRIVE_FOLDER`               | Google Drive folder name                  |
| `S3_ENDPOINT`                 | S3-compatible endpoint                    |
| `S3_ACCESS_KEY`               | S3 access key                             |
| `S3_SECRET_KEY`               | S3 secret key                             |
| `S3_BUCKET`                   | S3 bucket name                            |
| `S3_REGION`                   | S3 region (default: `us-east-1`)          |
| `SYNC_CRON_SCHEDULE`          | Cron expression (default: `0 6,18 * * *`) |

## Schedule Examples

| Cron Expression | Description                |
| --------------- | -------------------------- |
| `0 6,18 * * *`  | 6 AM and 6 PM UTC          |
| `0 */6 * * *`   | Every 6 hours              |
| `*/30 * * * *`  | Every 30 minutes (testing) |

## API Endpoints

| Endpoint     | Method | Description         |
| ------------ | ------ | ------------------- |
| `/health`    | GET    | Health check        |
| `/sync`      | POST   | Trigger manual sync |
| `/task/{id}` | GET    | Get task status     |

```bash
curl -X POST http://localhost:8080/sync -H "Content-Type: application/json" -d '{"dry_run": false}'
```

## Monitoring

```bash
docker compose --profile monitoring up -d   # Flower at http://localhost:5555
docker compose logs -f worker               # View worker logs
```

## Troubleshooting

```bash
docker compose exec worker python -c "
from shared import sync_service
sync_service.setup()
print(sync_service.test_connections())
"
```

- **Google Drive**: Verify JSON is valid, service account has folder access
- **S3**: Check endpoint URL includes `https://`, verify credentials
- **Schedule**: Check cron format, verify worker running with `docker compose ps`
