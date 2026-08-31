FROM python:3.12-slim AS runtime

ARG RELEASE_VERSION=rc-1.1.0
ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="Vayobyzh API" \
      org.opencontainers.image.description="API and Kytchi recommendation workers" \
      org.opencontainers.image.source="https://github.com/senseigus1-stack/events_soft" \
      org.opencontainers.image.version="${RELEASE_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
RUN groupadd --system app && useradd --system --gid app --home /app app

COPY pyproject.toml ./
COPY requirements.lock ./
COPY events_api ./events_api
COPY alembic.ini ./
COPY migrations ./migrations
RUN pip install --upgrade pip \
    && pip install --requirement requirements.lock \
    && pip install --no-deps .

USER app
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3)"
CMD ["uvicorn", "events_api.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips=*"]
