#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

TOPOLOGY_JAR="storm/topology/crime-alert-topology.jar"
TOPOLOGY_CLASS="com.project.CrimeAlertTopology"

if [[ ! -f "$TOPOLOGY_JAR" ]]; then
  echo "Storm topology artifact not found: $TOPOLOGY_JAR" >&2
  echo "Build or provide the topology implementation before running this target." >&2
  exit 1
fi

docker-compose exec storm-nimbus \
  storm jar "/app/$TOPOLOGY_JAR" "$TOPOLOGY_CLASS"
