# Task 1 — Dockerfile for the ingestor.
#
# Requirements:
# - Use the fixed Python 3.12 slim image.
# - Enable unbuffered Python output so container logs appear immediately.
# - Install dependencies in a separate layer before copying application code.
# - Run the application as a non-root user.
# - Create /cache and make it writable by the application user.
# - Use an exec-form ENTRYPOINT to run app/gh_ingest.py.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY app/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app/ /app/app/

RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /cache \
    && chown -R appuser:appuser /cache /app

USER appuser

ENTRYPOINT ["python", "app/gh_ingest.py"]