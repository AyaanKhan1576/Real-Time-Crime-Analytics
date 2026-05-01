up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

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
