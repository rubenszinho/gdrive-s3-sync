FROM python:3.11-slim

LABEL service="sync-worker"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -O https://downloads.rclone.org/rclone-current-linux-amd64.zip \
    && unzip rclone-current-linux-amd64.zip \
    && cp rclone-*-linux-amd64/rclone /usr/bin/ \
    && chmod 755 /usr/bin/rclone \
    && rm -rf rclone-*

RUN useradd -m -s /bin/bash appuser

WORKDIR /app

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY worker/*.py .

RUN mkdir -p /tmp/rclone && chown appuser:appuser /tmp/rclone

USER appuser

CMD ["celery", "-A", "celery_app", "worker", "--beat", "--loglevel=info", "--concurrency=1", "-Q", "sync"]
