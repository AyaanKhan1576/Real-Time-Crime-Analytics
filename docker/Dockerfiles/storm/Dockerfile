FROM python:3.10-slim

WORKDIR /app

# Install runtime deps used by the harness and bolts
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev build-essential ca-certificates && \
    pip install --no-cache-dir pyyaml kafka-python psycopg2-binary pymongo && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy project files into container (mounted as read-only by compose)
COPY . /app
ENV PYTHONPATH=/app

VOLUME ["/app/logs"]

CMD ["bash", "-c", "python scripts/integration_smoke_test.py"]
