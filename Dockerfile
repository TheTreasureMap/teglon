# Teglon with the code baked in, for Kubernetes. The default command serves the gwtm-facing API; the same image
# runs the CLI (`python -m web.src.services.teglon_cli ...`) for the tm-ingest CronJob.
# Mount persistent data over:
#   /app/web/events                   per-event maps, tile tables, contour caches
#   /app/web/src/utilities/pickles    composed_completeness_dict.pkl and friends
#   /dustmaps                         SFD maps (DUSTMAPS_CONFIG_FNAME points at its config)
# docker/teglon_worker/Dockerfile is the local dev image, which mounts the repo instead.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential pkg-config default-libmysqlclient-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app \
    DUSTMAPS_CONFIG_FNAME=/dustmaps/dustmaps_config.json

COPY pyproject.toml README.md ./
COPY web ./web
RUN pip install --no-cache-dir ".[api]"
COPY . .

RUN useradd --uid 1000 --create-home teglon && chown -R teglon /app
USER teglon

EXPOSE 8000
# One worker: the API keeps its run records in memory (see web/project/__init__.py).
CMD ["uvicorn", "web.project:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
