#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-data}"
MAX_ROWS="${MAX_ROWS:-0}"
USE_API="${USE_API:-0}"
FORCE="${FORCE:-0}"
APP_TOKEN="${SOCRATA_APP_TOKEN:-}"

mkdir -p "$OUT_DIR"

DATASETS=(
  "crimes|ijzp-q8t2|crimes.csv"
  "police_stations|z8bn-74gv|police_stations.csv"
  "arrests|dpt3-jri9|arrests.csv"
  "violence_reduction|gumc-mgzr|violence.csv"
  "sex_offenders|vc9r-bqvy|sex_offenders.csv"
)

build_url() {
  local dataset_id="$1"
  if [[ "$USE_API" == "1" ]]; then
    if [[ "$MAX_ROWS" -gt 0 ]]; then
      printf "https://data.cityofchicago.org/resource/%s.csv?\$limit=%s" "$dataset_id" "$MAX_ROWS"
    else
      printf "https://data.cityofchicago.org/resource/%s.csv" "$dataset_id"
    fi
  else
    printf "https://data.cityofchicago.org/api/views/%s/rows.csv?accessType=DOWNLOAD" "$dataset_id"
  fi
}

echo "Downloading City of Chicago datasets..."
echo "OUT_DIR=$OUT_DIR"
if [[ "$USE_API" == "1" ]]; then
  echo "MODE=API CSV"
else
  echo "MODE=Export CSV"
fi
if [[ "$MAX_ROWS" -gt 0 ]]; then
  echo "MAX_ROWS=$MAX_ROWS"
fi

for item in "${DATASETS[@]}"; do
  IFS='|' read -r name id file <<< "$item"
  url="$(build_url "$id")"
  out_file="$OUT_DIR/$file"

  if [[ -f "$out_file" && "$FORCE" != "1" ]]; then
    echo "[SKIP] $name: file exists -> $out_file"
    continue
  fi

  echo "[GET ] $name from $url"
  if [[ -n "$APP_TOKEN" ]]; then
    curl -L --fail --retry 4 --retry-delay 2 -H "X-App-Token: $APP_TOKEN" -o "$out_file" "$url"
  else
    curl -L --fail --retry 4 --retry-delay 2 -o "$out_file" "$url"
  fi

  if [[ -s "$out_file" ]]; then
    size_mb=$(awk -v b="$(wc -c < "$out_file")" 'BEGIN { printf "%.2f", b/1024/1024 }')
    echo "[OK  ] $name saved to $out_file (${size_mb}MB)"
  else
    echo "[FAIL] $name produced an empty file"
    exit 1
  fi
done

echo "All requested downloads completed."
