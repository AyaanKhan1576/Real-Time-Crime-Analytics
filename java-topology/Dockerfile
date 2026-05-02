FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY java-topology/pom.xml /src/pom.xml
COPY java-topology/src /src/src
RUN mvn -q -DskipTests package

FROM storm:2.6.0
WORKDIR /app

USER root
RUN mkdir -p /var/lib/storm && chown -R storm:storm /var/lib/storm

COPY --from=build /src/target/crime-alert-topology-1.0.0-all.jar /app/crime-alert-topology.jar
COPY config /app/config
COPY java-topology/storm.yaml /conf/storm.yaml
ENV STORM_CONF_DIR=/conf
USER storm
CMD ["storm", "jar", "/app/crime-alert-topology.jar", "com.project.CrimeAlertTopology"]
