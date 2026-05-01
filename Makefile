up:
	docker-compose -f docker/docker-compose.yml up

down:
	docker-compose -f docker/docker-compose.yml down

run-producer:
	python kafka/producer.py --config config/config.yaml

run-storm:
	@echo "Deploy Java topology in production:"
	@echo "storm jar storm/topology/crime-alert-topology.jar com.project.CrimeAlertTopology"
	@echo "For Python development: streamparse run storm.topology.crime_alert_topology.CrimeAlertTopology"

run-mongo-setup:
	python db/mongo_setup.py

validate-streaming:
	python scripts/validate_streaming.py
