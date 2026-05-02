FROM confluentinc/cp-kafka:7.4.0

# This Dockerfile wraps the upstream Confluent Kafka image so we can
# add project-specific configuration or tooling later without editing
# the central compose file.

ENV KAFKA_BROKER_ID=1
ENV KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1

EXPOSE 9092

CMD ["bash", "/etc/confluent/docker/run"]
