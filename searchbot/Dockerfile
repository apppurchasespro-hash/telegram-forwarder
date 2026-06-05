# Image for the search bot. Build from the REPO ROOT so the `searchbot`
# package and `downloader.py` (used by the indexer) are in context:
#   docker build -f searchbot/Dockerfile -t searchbot .
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    SEARCHBOT_CONFIG=/data/config.json

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

# .dockerignore keeps sessions/config/catalog/secrets out of the image.
COPY . .

# Runtime data (config.json + catalog.db) is mounted at /data, not baked in.
VOLUME ["/data"]

CMD ["python", "-m", "searchbot.bot"]
