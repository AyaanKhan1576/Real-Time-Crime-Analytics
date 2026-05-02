#!/usr/bin/env bash
set -euo pipefail

python kafka/producer.py --config config/config.yaml
