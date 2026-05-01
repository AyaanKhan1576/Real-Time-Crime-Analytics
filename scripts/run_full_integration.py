#!/usr/bin/env python3
"""
Full integration test: read CSV, run in-process harness, capture outputs to files.
Environment variables (optional):
  - SAMPLE_FRACTION: fraction of dataset to process (default: 0.25)
  - OUTPUT_DIR: where to write outputs (default: ./logs)
"""
import os
import sys
from pathlib import Path
import json

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from storm.harness.run_topology_runner import run_from_csv


def run_full_integration_test():
    sample_fraction = float(os.getenv('SAMPLE_FRACTION', '0.25'))
    output_dir = Path(os.getenv('OUTPUT_DIR', 'logs'))
    output_dir.mkdir(exist_ok=True, parents=True)

    csv_path = PROJECT_ROOT / 'data' / 'raw' / 'Crimes_-_2001_to_Present_20260501.csv'
    if not csv_path.exists():
        print(f'ERROR: dataset not found at {csv_path}')
        sys.exit(1)

    print(f'Full integration test: Harness processing + file outputs')
    print(f'  Sample: {sample_fraction*100}% of dataset')
    print(f'  Output: {output_dir}')

    # Run the in-process harness
    try:
        run_from_csv(
            str(csv_path),
            sample_fraction=sample_fraction,
            config_path='config/config.yaml',
            producer=None,
            output_dir=str(output_dir),
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

