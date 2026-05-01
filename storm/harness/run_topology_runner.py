"""
Local Storm topology runner (test-mode).
This runner emulates the Storm pipeline in-process for integration testing without a full Storm cluster.
It reads a CSV (or Kafka topic if configured) and routes tuples through the bolts in order:
ParseBolt -> DistrictBolt -> WindowBolt -> AnomalyBolt -> (Alert write emulated to files)
"""
import os
import sys
import csv
import json
import time
from pathlib import Path

# Ensure project root on path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from storm.bolts.parse_bolt import ParseBolt
from storm.bolts.district_bolt import DistrictBolt
from storm.bolts.window_bolt import WindowBolt
from storm.bolts.anomaly_bolt import AnomalyBolt


class SimpleTuple:
    def __init__(self, values):
        self.values = values


def run_from_csv(csv_path, sample_fraction=0.25, config_path='config/config.yaml', producer=None, output_dir=None):
    """
    Run in-process topology on CSV data.
    Args:
      csv_path: path to crimes CSV
      sample_fraction: fraction of rows to process (0.0-1.0)
      config_path: path to config.yaml
      producer: optional KafkaProducer to send events to (for full integration tests)
      output_dir: optional directory to write anomalies and counts to JSON files
    """
    # instantiate bolts
    conf = {'config_path': config_path}
    ctx = {}

    p = ParseBolt()
    d = DistrictBolt()
    w = WindowBolt()
    a = AnomalyBolt()

    # initialize and ensure safe defaults in case of configuration errors
    try:
        p.initialize(conf, ctx)
    except Exception as e:
        print('ParseBolt.initialize failed:', e)
    # ensure ParseBolt defaults
    if not hasattr(p, 'enable_raw_event_logging'):
        p.enable_raw_event_logging = False
        p.raw_coll = None

    try:
        w.initialize(conf, ctx)
    except Exception as e:
        print('WindowBolt.initialize failed:', e)
    # ensure WindowBolt defaults
    if not hasattr(w, 'buckets'):
        from collections import defaultdict, deque

        w.window_size = 300
        w.slide = 60
        w.buckets = defaultdict(deque)
        w.last_emit = defaultdict(int)

    try:
        a.initialize(conf, ctx)
    except Exception as e:
        print('AnomalyBolt.initialize failed:', e)
    # ensure AnomalyBolt defaults
    if not hasattr(a, 'threshold'):
        a.threshold = 25
        a.low = 25
        a.medium = 40
        a.high = 60

    alerts = []
    realtime_counts = []

    total = 0
    processed = 0
    # two-pass streaming: count lines, then process first `limit` rows without loading entire file
    with open(csv_path, newline='', encoding='utf-8') as fh:
        # count non-header rows
        reader = csv.reader(fh)
        total = sum(1 for _ in reader) - 1

    limit = max(1, int(total * sample_fraction))
    print(f"CSV rows: {total}, processing ~{limit}")

    with open(csv_path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for i, row in enumerate(reader):
            if i >= limit:
                break
            # Build message matching producer JSON
            msg = json.dumps({
                'case_number': row.get('ID') or row.get('Case Number') or row.get('case_number'),
                'date': row.get('Date') or row.get('date'),
                'block': row.get('Block') or row.get('block'),
                'primary_type': row.get('Primary Type') or row.get('primary_type'),
                'district': row.get('District') or row.get('district'),
                'arrest': row.get('Arrest') in ['true', 'True', '1', 't', 'yes', 'Y', 'y'],
                'latitude': row.get('Latitude') or row.get('latitude') or 0.0,
                'longitude': row.get('Longitude') or row.get('longitude') or 0.0,
            })

            # Pass through ParseBolt
            t = SimpleTuple([msg])
            try:
                p.process(t)
                # Prefer to use ParseBolt emitted output if available (Bolt shim stores emits in _emitted)
                parsed = None
                p_emitted = getattr(p, '_emitted', [])
                if p_emitted:
                    # ParseBolt emits a list [data], so take the first emit and its first element
                    first = p_emitted.pop(0)
                    if isinstance(first, list) and first:
                        parsed = first[0]
                if parsed is None:
                    # fallback to re-parsing the original message
                    parsed = json.loads(msg)
                # DistrictBolt
                td = SimpleTuple([parsed])
                d.process(td)

                # DistrictBolt emits [district, data]
                district = str(parsed.get('district') or 'UNKNOWN')
                wd = SimpleTuple([district, parsed])
                w.process(wd)

                # WindowBolt may emit [district, count, window_start, window_end]
                # Anomaly detection is performed by AnomalyBolt in initialize/process chain.
                # For this in-process runner we do not call AlertBolt (DB writes), instead
                # we collect anomalies from the AnomalyBolt's internal emitted list if present.
                # Run anomaly bolt using the most recent emit from window bolt if available.
                # The simple Bolt shim appends emits to bolt._emitted; capture and clear them.
                emitted = getattr(w, '_emitted', [])
                if emitted:
                    for out in emitted:
                        try:
                            # out expected: [district, count, window_start, window_end]
                            t_an = SimpleTuple(out)
                            a.process(t_an)
                            # capture anomalies
                            a_emitted = getattr(a, '_emitted', [])
                            if a_emitted:
                                for ae in a_emitted:
                                    alerts.append(ae)
                                a._emitted = []
                        except Exception:
                            pass
                    w._emitted = []
            except Exception as exc:
                print('parse error', exc)

            processed += 1

    print(f"Processed {processed} rows.")

    # Write outputs to files if specified
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True, parents=True)

        # Write anomalies
        alerts_file = output_path / 'anomalies.jsonl'
        with open(alerts_file, 'w') as fh:
            for alert in alerts:
                if isinstance(alert, list) and alert:
                    alert_dict = alert[0] if isinstance(alert[0], dict) else alert
                else:
                    alert_dict = alert
                fh.write(json.dumps(alert_dict) + '\n')
        print(f'  Wrote {len(alerts)} anomalies to {alerts_file}')

        # Write summary
        summary_file = output_path / 'summary.json'
        with open(summary_file, 'w') as fh:
            summary = {
                'timestamp': time.time(),
                'total_rows_processed': processed,
                'anomalies_detected': len(alerts),
                'sample_fraction': sample_fraction,
            }
            json.dump(summary, fh, indent=2)
        print(f'  Wrote summary to {summary_file}')


if __name__ == '__main__':
    # default dataset path
    ROOT = Path(PROJECT_ROOT)
    csv_path = ROOT / 'data' / 'raw' / 'Crimes_-_2001_to_Present_20260501.csv'
    if not csv_path.exists():
        print('CSV not found:', csv_path)
        sys.exit(1)
    run_from_csv(str(csv_path), sample_fraction=0.25, config_path='config/config.yaml')
