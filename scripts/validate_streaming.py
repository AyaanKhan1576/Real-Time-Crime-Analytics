import os
import sys


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config_loader import load_config


REQUIRED = {
    "kafka": ["bootstrap_servers", "topic_crime_events", "producer_rate_per_second"],
    "storm": ["window_size_seconds", "slide_interval_seconds", "anomaly_threshold", "severity"],
    "postgres": ["host", "port", "database", "user", "password"],
    "mongodb": ["host", "port", "database", "raw_events_collection", "alert_logs_collection"],
}


def main() -> None:
    cfg = load_config("config/config.yaml")

    missing = []
    for section, keys in REQUIRED.items():
        if section not in cfg:
            missing.append(f"Missing section: {section}")
            continue
        for key in keys:
            if key not in cfg[section]:
                missing.append(f"Missing key: {section}.{key}")

    if missing:
        print("Config validation failed:")
        for item in missing:
            print(f"- {item}")
        raise SystemExit(1)

    print("Config validation passed.")


if __name__ == "__main__":
    main()
