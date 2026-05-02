#!/usr/bin/env python3
"""
Run a local integration smoke test: process ~25% of the main crimes CSV through the in-process topology runner.
Writes a short summary to stdout.
"""
import os
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from storm.harness.run_topology_runner import run_from_csv
from config.config_loader import load_config


FALLBACK_CRIME_FILE = "Crimes_-_2001_to_Present_20260501.csv"


def resolve_crime_csv(config_path):
    cfg = load_config(config_path)
    data_cfg = cfg.get("data", {})
    base_path = Path(data_cfg.get("base_path", "data"))
    configured_file = data_cfg.get("crime_file", "crimes.csv")
    candidates = [
        base_path / configured_file,
        PROJECT_ROOT / "data" / configured_file,
        PROJECT_ROOT / "data" / "raw" / configured_file,
        base_path / FALLBACK_CRIME_FILE,
        PROJECT_ROOT / "data" / FALLBACK_CRIME_FILE,
        PROJECT_ROOT / "data" / "raw" / FALLBACK_CRIME_FILE,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def main():
    config_path = os.getenv("CONFIG_PATH", "config/config.yaml")
    csv_path = resolve_crime_csv(config_path)
    if not csv_path.exists():
        print('ERROR: dataset not found at', csv_path)
        sys.exit(2)
    print('Starting smoke test on ~25% of dataset...')
    run_from_csv(str(csv_path), sample_fraction=0.25, config_path=config_path)
    print('Smoke test finished.')


if __name__ == '__main__':
    main()
