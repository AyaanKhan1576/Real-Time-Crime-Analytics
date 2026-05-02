#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT_DIR/dist/storm_topology_bundle.zip"
mkdir -p "$(dirname "$OUT")"
echo "Packaging storm Python topology into $OUT"
cd "$ROOT_DIR"
zip -r "$OUT" storm -x "**/__pycache__/**" "**/*.pyc"
echo "Packaged: $OUT"
