#!/usr/bin/env python3
"""
Full integration test: read CSV, send to Kafka, run in-process harness, capture outputs to files.
Environment variables (optional):
  - KAFKA_BOOTSTRAP_SERVERS: Kafka endpoint (default: localhost:9092)
  - SAMPLE_FRACTION: fraction of dataset to process (default: 0.25)
  - OUTPUT_DIR: where to write outputs (default: ./logs)
"""
import os
import sys
from pathlib import Path
import json
import csv
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from kafka import KafkaProducer
from storm.harness.run_topology_runner import run_from_csv

def run_full_integration_test():
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    sample_fraction = float(os.getenv('SAMPLE_FRACTION', '0.25'))
    output_dir = Path(os.getenv('OUTPUT_DIR', 'logs'))
    output_dir.mkdir(exist_ok=True, parents=True)

    csv_path = PROJECT_ROOT / 'data' / 'raw' / 'Crimes_-_2001_to_Present_20260501.csv'
    if not csv_path.exists():
        print(f'ERROR: dataset not found at {csv_path}')
        sys.exit(1)

    print(f'Full integration test: Kafka → harness → file outputs')
    print(f'  Kafka: {kafka_bootstrap}')
    print(f'  Sample: {sample_fraction*100}% of dataset')
    print(f'  Output: {output_dir}')

    # Initialize Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            acks='all',
            retries=3,
        )
        print('✓ Connected to Kafka')
    except Exception as e:
        print(f'✗ Failed to connect to Kafka: {e}')
        print('  Continuing in offline mode (harness only, no Kafka writes)')
        producer = None

    # Run the in-process harness
    try:
        run_from_csv(
            str(csv_path),
            sample_fraction=sample_fraction,
            config_path='config/config.yaml',
            producer=producer,
            output_dir=str(output_dir),
        )
        print('✓ Harness processing completed')
    except Exception as e:
        print(f'✗ Harness failed: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if producer:
            producer.close()

    # Print summary
    print(f'\n✓ Integration test complete. Outputs in {output_dir}/')
    for f in sorted(output_dir.glob('*.json')):
        lines = len(f.read_text().strip().split('\n'))
        print(f'  - {f.name}: {lines} records')

if __name__ == '__main__':
    run_full_integration_test()
