"""
Local streaming test harness for Person 2 (Kafka + Storm) pipeline.

Usage:
  python scripts/streaming_local_test.py --config config/config.yaml --max-rows 200

This script simulates the Kafka producer + Storm bolts pipeline in-process
and writes outputs to `outputs/alerts.jsonl` and `outputs/realtime_counts.jsonl`.
It does not require Kafka, Storm, Postgres or MongoDB.
"""
import csv
import json
import os
import time
import argparse
from collections import defaultdict, deque
from datetime import datetime

from config.config_loader import load_config


def as_bool(value):
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "y", "yes", "t"}


def as_float(value, default=0.0):
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def read_any(row, keys):
    for key in keys:
        if key in row and row[key] is not None and str(row[key]).strip() != "":
            return row[key]
    return None


def parse_row(row):
    # Normalize incoming row keys to snake_case lowercase for robust matching
    norm = {}
    for k, v in row.items():
        if k is None:
            continue
        nk = k.strip().lower().replace(' ', '_')
        norm[nk] = v

    case_number = read_any(norm, ["case_number"])
    date_value = read_any(norm, ["date"])
    block_value = read_any(norm, ["block"])
    primary_type = read_any(norm, ["primary_type"])
    district = read_any(norm, ["district", "beat"])  # fall back to beat if district missing
    arrest_value = read_any(norm, ["arrest"])
    latitude = read_any(norm, ["latitude"])
    longitude = read_any(norm, ["longitude"])

    if not case_number or not date_value or not block_value or not primary_type:
        return None

    msg = {
        "case_number": str(case_number).strip(),
        "date": str(date_value).strip(),
        "block": str(block_value).strip(),
        "primary_type": str(primary_type).strip(),
        "district": str(district).strip() if district is not None and str(district).strip() else "UNKNOWN",
        "arrest": as_bool(arrest_value),
        "latitude": as_float(latitude),
        "longitude": as_float(longitude),
    }
    return msg


class WindowManager:
    def __init__(self, window_size, slide):
        self.window_size = int(window_size)
        self.slide = int(slide)
        self.buckets = defaultdict(deque)
        self.last_emit = defaultdict(int)

    def ingest(self, district, ts=None):
        now = int(ts or time.time())
        dq = self.buckets[district]
        dq.append(now)
        # remove old
        while dq and dq[0] < now - self.window_size:
            dq.popleft()

        # decide whether to emit
        if self.last_emit[district] and now - self.last_emit[district] < self.slide:
            return None

        self.last_emit[district] = now
        return {
            "district": district,
            "count": len(dq),
            "window_start": now - self.window_size,
            "window_end": now,
        }


def severity_from_count(count, low, medium, high):
    if count >= high:
        return "HIGH"
    if count >= medium:
        return "MEDIUM"
    if count >= low:
        return "LOW"
    return None


def ensure_out_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def run_local_pipeline(config_path, max_rows=None, out_dir="outputs", rate=0):
    cfg = load_config(config_path)
    data_cfg = cfg.get("data", {})
    storm_cfg = cfg.get("storm", {})

    crime_file = os.path.join(data_cfg.get("base_path", "data"), data_cfg.get("crime_file"))
    if not os.path.exists(crime_file):
        # fallback to repo data/raw folder
        crime_file = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "data", "raw", data_cfg.get("crime_file")))

    # If the configured filename is not present, try to find any sensible crime CSV in data/raw
    if not os.path.exists(crime_file):
        raw_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
        if os.path.isdir(raw_dir):
            for f in os.listdir(raw_dir):
                if f.lower().endswith('.csv') and 'crime' in f.lower():
                    crime_file = os.path.join(raw_dir, f)
                    break

    if not os.path.exists(crime_file):
        raise FileNotFoundError(f"Crime file not found: {crime_file}")

    window_size = int(storm_cfg.get("window_size_seconds", 300))
    slide = int(storm_cfg.get("slide_interval_seconds", 60))
    threshold = int(storm_cfg.get("anomaly_threshold", 25))
    sev = storm_cfg.get("severity", {})
    low_min = int(sev.get("low_min", 25))
    medium_min = int(sev.get("medium_min", 40))
    high_min = int(sev.get("high_min", 60))

    ensure_out_dir(out_dir)
    alerts_file = os.path.join(out_dir, "alerts.jsonl")
    counts_file = os.path.join(out_dir, "realtime_counts.jsonl")
    mongo_file = os.path.join(out_dir, "mongo_alerts.jsonl")

    wm = WindowManager(window_size, slide)
    sent = 0

    with open(crime_file, newline='', encoding='utf-8') as csvfile, \
            open(alerts_file, 'w', encoding='utf-8') as af, \
            open(counts_file, 'w', encoding='utf-8') as cf, \
            open(mongo_file, 'w', encoding='utf-8') as mf:

        reader = csv.DictReader(csvfile)
        for row in reader:
            msg = parse_row(row)
            if msg is None:
                continue

            # DistrictBolt
            district = str(msg.get('district') or 'UNKNOWN').strip() or 'UNKNOWN'

            # WindowBolt ingest and possibly emit
            emit = wm.ingest(district)
            if emit is not None:
                # AnomalyBolt
                cnt = int(emit['count'])
                severity = severity_from_count(cnt, low_min, medium_min, high_min)

                # Always write realtime count record
                count_doc = {
                    'district': district,
                    'window_start': datetime.utcfromtimestamp(emit['window_start']).isoformat(),
                    'window_end': datetime.utcfromtimestamp(emit['window_end']).isoformat(),
                    'event_count': cnt,
                    'updated_at': datetime.utcnow().isoformat(),
                }
                cf.write(json.dumps(count_doc) + "\n")

                if severity is not None:
                    ws = datetime.utcfromtimestamp(emit['window_start']).strftime('%Y%m%dT%H%M%S')
                    we = datetime.utcfromtimestamp(emit['window_end']).strftime('%Y%m%dT%H%M%S')
                    alert_id = f"{district}_{ws}_{we}"
                    alert_doc = {
                        'alert_id': alert_id,
                        'district': district,
                        'timestamp': datetime.utcnow().isoformat(),
                        'window_start': datetime.utcfromtimestamp(emit['window_start']).isoformat(),
                        'window_end': datetime.utcfromtimestamp(emit['window_end']).isoformat(),
                        'event_count': cnt,
                        'threshold': threshold,
                        'severity': severity,
                        'message': f"Crime spike detected in district {district}",
                        'source': 'local-sim',
                    }
                    af.write(json.dumps(alert_doc) + "\n")
                    mf.write(json.dumps(alert_doc) + "\n")

            sent += 1
            if max_rows and sent >= max_rows:
                break

            if rate and rate > 0:
                time.sleep(1.0 / rate)

    print(f"Local pipeline finished. Sent ~{sent} events. Outputs in: {out_dir}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run local streaming pipeline simulation')
    parser.add_argument('--config', default='config/config.yaml')
    parser.add_argument('--max-rows', type=int, default=200)
    parser.add_argument('--out-dir', default='outputs')
    parser.add_argument('--rate', type=float, default=0)
    args = parser.parse_args()

    run_local_pipeline(args.config, max_rows=args.max_rows, out_dir=args.out_dir, rate=args.rate)
