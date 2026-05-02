#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

TOPOLOGY_NAME="${STORM_TOPOLOGY_NAME:-crime-alert-topology}"
COMPOSE=(docker compose -f docker-compose.yml)

wait_for_nimbus() {
  local attempts="${1:-24}"
  local delay_seconds="${2:-5}"
  local output_file="/tmp/crime_analytics_storm_list.out"

  for _ in $(seq 1 "$attempts"); do
    if "${COMPOSE[@]}" exec -T storm-nimbus storm list >"$output_file" 2>&1; then
      return 0
    fi
    sleep "$delay_seconds"
  done

  cat "$output_file" >&2 || true
  return 1
}

recover_storm_cluster() {
  echo "Storm leader is not healthy. Recreating Zookeeper/Kafka/Storm containers to clear stale runtime state..."
  docker rm -f kafka-producer storm-submit-java storm-supervisor storm-ui storm-nimbus kafka zookeeper >/dev/null 2>&1 || true
  "${COMPOSE[@]}" up -d --build zookeeper kafka storm-nimbus storm-supervisor storm-ui
}

"${COMPOSE[@]}" up -d --build zookeeper kafka storm-nimbus storm-supervisor storm-ui

if ! wait_for_nimbus 18 5; then
  recover_storm_cluster
  wait_for_nimbus 30 5
fi

if "${COMPOSE[@]}" exec -T storm-nimbus storm list | awk '{print $1}' | grep -qx "$TOPOLOGY_NAME"; then
  echo "Existing Storm topology '$TOPOLOGY_NAME' found. Stopping it before redeploy..."
  "${COMPOSE[@]}" exec -T storm-nimbus storm kill "$TOPOLOGY_NAME" -w 0 || true
  sleep 12
fi

"${COMPOSE[@]}" --profile java-submit up --build --force-recreate storm-submit-java

submit_status="$(docker inspect storm-submit-java --format '{{.State.ExitCode}}' 2>/dev/null || echo 1)"
if [[ "$submit_status" != "0" ]]; then
  echo "Storm topology submission failed with exit code $submit_status." >&2
  docker logs --tail 160 storm-submit-java >&2 || true
  exit "$submit_status"
fi

if ! wait_for_nimbus 12 5; then
  echo "Storm Nimbus did not stay healthy after topology submission." >&2
  exit 1
fi

if ! "${COMPOSE[@]}" exec -T storm-nimbus storm list | awk '{print $1}' | grep -qx "$TOPOLOGY_NAME"; then
  echo "Storm topology '$TOPOLOGY_NAME' was not found after submission." >&2
  exit 1
fi

echo "Storm topology submission finished."
echo "Storm UI: http://localhost:8088"
