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


run-dashboard:
	./scripts/run_dashboard.sh

run-producer:
	python kafka/producer.py --config config/config.yaml

run-storm:
	@echo "Start the cluster with: docker compose -f docker/docker-compose.yml up -d"
	@echo "Then submit the topology with: streamparse run storm.topology.crime_alert_topology.CrimeAlertTopology"

run-storm-cluster:
	docker compose -f docker/docker-compose.yml up -d storm-nimbus storm-supervisor storm-ui

run-storm-submit:
	docker compose -f docker/docker-compose.yml --profile java-submit up -d storm-submit-java

run-mongo-setup:
	python db/mongo_setup.py

validate-streaming:
	python scripts/validate_streaming.py
