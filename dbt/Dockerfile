# syntax=docker/dockerfile:1
FROM python:3.11-slim
ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get -y upgrade && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir dbt-core==1.7.15 dbt-postgres==1.7.15
WORKDIR /app
ENTRYPOINT ["dbt"]
