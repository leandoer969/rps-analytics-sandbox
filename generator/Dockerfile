# syntax=docker/dockerfile:1
FROM python:3.11-slim@sha256:0ce77749ac83174a31d5e107ce0cfa6b28a2fd6b0615e029d9d84b39c48976ee

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Pull Debian security fixes present at build time
RUN apt-get update \
    && apt-get -y upgrade \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Drop privileges
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

COPY generate.py .
CMD ["python", "generate.py"]
