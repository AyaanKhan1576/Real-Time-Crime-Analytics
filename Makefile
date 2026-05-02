.PHONY: up up-detached down build logs ps spark-up spark-run spark-run-15pct run-spark run-spark-15pct run-producer run-storm run-dashboard setup-mongo run-all run-all-15pct validate-streaming logs-producer logs-storm logs-dashboard logs-spark

COMPOSE_FILE ?= docker-compose.yml
SPARK_CONFIG ?= /app/config/config.yaml
SPARK_15PCT_CONFIG ?= /app/config/config.spark_15pct.yaml

up:
	docker compose -f $(COMPOSE_FILE) up --build

up-detached:
	docker compose -f $(COMPOSE_FILE) up -d --build postgres mongodb zookeeper kafka storm-nimbus storm-supervisor storm-ui spark-master spark-worker streamlit

down:
	docker compose -f $(COMPOSE_FILE) down

build:
	docker compose -f $(COMPOSE_FILE) build

logs:
	docker compose -f $(COMPOSE_FILE) logs -f

ps:
	docker compose -f $(COMPOSE_FILE) ps

spark-up:
	docker compose -f $(COMPOSE_FILE) up -d --build postgres mongodb spark-master spark-worker streamlit

spark-run:
	bash ./scripts/run_spark_batch.sh $(SPARK_CONFIG)

spark-run-15pct:
	bash ./scripts/run_spark_batch.sh $(SPARK_15PCT_CONFIG)

run-spark:
	bash ./scripts/run_spark_batch.sh $(SPARK_CONFIG)

run-spark-15pct:
	bash ./scripts/run_spark_batch.sh $(SPARK_15PCT_CONFIG)

run-dashboard:
	bash ./scripts/run_dashboard.sh

run-storm:
	bash ./scripts/run_storm_topology.sh

run-producer:
	bash ./scripts/run_kafka_producer.sh

setup-mongo:
	bash ./scripts/setup_mongo.sh

run-all: up-detached setup-mongo run-storm run-producer spark-run run-dashboard

run-all-15pct: up-detached setup-mongo run-storm run-producer spark-run-15pct run-dashboard

validate-streaming:
	docker compose -f $(COMPOSE_FILE) run --rm storm-harness

logs-producer:
	docker compose -f $(COMPOSE_FILE) logs -f kafka-producer

logs-storm:
	docker compose -f $(COMPOSE_FILE) logs -f storm-nimbus storm-supervisor storm-ui storm-submit-java

logs-dashboard:
	docker compose -f $(COMPOSE_FILE) logs -f streamlit

logs-spark:
	tail -f "$$(ls -t logs/spark/*.log | head -1)"
