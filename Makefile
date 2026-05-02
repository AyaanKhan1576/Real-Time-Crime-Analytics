.PHONY: up down build logs spark-up spark-run spark-run-15pct run-spark run-spark-15pct run-producer run-storm run-dashboard setup-mongo ps

SPARK_CONFIG ?= /app/config/config.yaml
SPARK_15PCT_CONFIG ?= /app/config/config.spark_15pct.yaml

up:
	docker compose -f docker-compose.yml up --build

down:
	docker compose -f docker-compose.yml down

build:
	docker compose -f docker-compose.yml build

logs:
	docker compose -f docker-compose.yml logs -f

ps:
	docker compose -f docker-compose.yml ps

spark-up:
	docker compose -f docker-compose.yml up -d --build postgres spark-master spark-worker
	docker compose -f docker-compose.yml up -d --build --no-deps streamlit

spark-run:
	./scripts/run_spark_batch.sh $(SPARK_CONFIG)

spark-run-15pct:
	./scripts/run_spark_batch.sh $(SPARK_15PCT_CONFIG)

run-spark:
	./scripts/run_spark_batch.sh $(SPARK_CONFIG)

run-spark-15pct:
	./scripts/run_spark_batch.sh $(SPARK_15PCT_CONFIG)

run-producer:
	./scripts/run_kafka_producer.sh

run-storm:
	./scripts/run_storm_topology.sh

run-dashboard:
	./scripts/run_dashboard.sh

setup-mongo:
	./scripts/setup_mongo.sh
