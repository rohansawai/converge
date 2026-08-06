FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-dev.txt pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt \
    && pip install --no-cache-dir -e . --no-deps

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src
