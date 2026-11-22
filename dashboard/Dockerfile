# syntax=docker/dockerfile:1
FROM python:3.13-slim

WORKDIR /app

# Build context is the project root; only the dashboard package is needed.
COPY dashboard/ /app/dashboard/

# Run as a non-root user.
RUN groupadd -r dashboard && useradd -r -g dashboard -d /app -s /bin/false dashboard

# The swarm directory is mounted read-only at runtime (e.g. /data/swarm:ro).
ENV SWARM_DIR=/data/swarm \
    HOST=0.0.0.0 \
    PORT=8787 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8787

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8787/healthz', timeout=5)" || exit 1

USER dashboard

CMD ["python", "-m", "dashboard.server"]
