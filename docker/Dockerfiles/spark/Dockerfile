FROM apache/spark:3.5.1

USER root

ENV PYTHONPATH=/app
WORKDIR /app

COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN python3 -m pip install --upgrade pip \
    && python3 -m pip install --no-cache-dir -r /tmp/requirements-spark.txt

USER spark
