#!/usr/bin/env python3
"""
Full integration test: read CSV, run in-process harness, capture outputs to files.
Environment variables (optional):
    - SAMPLE_FRACTION: fraction of dataset to process if MAX_ROWS is unset (default: 0.25)
    - MAX_ROWS: fixed row cap for fast streaming smoke tests (default: 5000)
    - OUTPUT_DIR: where to write outputs (default: ./logs)
"""
import os
import sys
from pathlib import Path
import json

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


def run_full_integration_test():
    sample_fraction = float(os.getenv('SAMPLE_FRACTION', '0.25'))
    max_rows = os.getenv('MAX_ROWS', '20000')
    max_rows = int(max_rows) if str(max_rows).strip() else None
    persist_to_dbs = os.getenv('PERSIST_TO_DBS', '1').strip().lower() not in {'0', 'false', 'no', 'off'}
    output_dir = Path(os.getenv('OUTPUT_DIR', 'logs'))
    output_dir.mkdir(exist_ok=True, parents=True)

    config_path = os.getenv("CONFIG_PATH", "config/config.yaml")
    csv_path = resolve_crime_csv(config_path)
    if not csv_path.exists():
        print(f'ERROR: dataset not found at {csv_path}')
        sys.exit(1)

    print(f'Full integration test: Harness processing + file outputs')
    if max_rows is not None:
        print(f'  Max rows: {max_rows}')
    else:
        print(f'  Sample: {sample_fraction*100}% of dataset')
    print(f'  Output: {output_dir}')

    # Run the in-process harness
    try:
        run_from_csv(
            str(csv_path),
            sample_fraction=sample_fraction,
            config_path=config_path,
            producer=None,
            output_dir=str(output_dir),
            persist_to_dbs=persist_to_dbs,
            max_rows=max_rows,
        )
        print('✓ Harness processing completed')
    except Exception as e:
        print(f'✗ Harness failed: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Print summary
    print(f'\n✓ Integration test complete. Outputs in {output_dir}/')
    for f in sorted(output_dir.glob('*.json')):
        lines = len(f.read_text().strip().split('\n'))
        print(f'  - {f.name}: {lines} records')


if __name__ == '__main__':
    run_full_integration_test()
