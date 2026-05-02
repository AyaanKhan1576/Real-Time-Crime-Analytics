#!/usr/bin/env python3
from pathlib import Path
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from storm.harness.run_topology_runner import run_from_csv

csv_path = PROJECT_ROOT / 'data' / 'raw' / 'Crimes_-_2001_to_Present_20260501.csv'
if not csv_path.exists():
    print('CSV not found:', csv_path)
    sys.exit(1)

print('Running quick smoke (1% of dataset)...')
run_from_csv(str(csv_path), sample_fraction=0.01, config_path='config/config.yaml')
print('Quick smoke finished')
