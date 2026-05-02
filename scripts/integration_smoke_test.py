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


def main():
    csv_path = PROJECT_ROOT / 'data' / 'raw' / 'Crimes_-_2001_to_Present_20260501.csv'
    if not csv_path.exists():
        print('ERROR: dataset not found at', csv_path)
        sys.exit(2)
    print('Starting smoke test on ~25% of dataset...')
    run_from_csv(str(csv_path), sample_fraction=0.25, config_path='config/config.yaml')
    print('Smoke test finished.')


if __name__ == '__main__':
    main()
