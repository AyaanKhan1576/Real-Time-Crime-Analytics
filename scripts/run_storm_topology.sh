#!/usr/bin/env bash
set -euo pipefail

# For Java topology deployment (final target expected by assignment):
# storm jar storm/topology/crime-alert-topology.jar com.project.CrimeAlertTopology

# Python/streamparse development entrypoint placeholder:
streamparse run storm.topology.crime_alert_topology.CrimeAlertTopology
