"""
AnomalyBolt skeleton: compare counts against threshold and emit anomalies.
"""
from streamparse.bolt import Bolt
import os
import sys


def _load_cfg(config_path):
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from config.config_loader import load_config

    return load_config(config_path)


class AnomalyBolt(Bolt):
    def initialize(self, conf, ctx):
        cfg_path = conf.get("config_path", "config/config.yaml")
        cfg = _load_cfg(cfg_path)
        self.threshold = int(cfg.get('storm', {}).get('anomaly_threshold', 25))
        sev = cfg.get('storm', {}).get('severity', {})
        self.low = int(sev.get('low_min', 25))
        self.medium = int(sev.get('medium_min', 40))
        self.high = int(sev.get('high_min', 60))

    def process(self, tup):
        district, count, window_start, window_end = tup.values
        severity = None
        if count >= self.high:
            severity = 'HIGH'
        elif count >= self.medium:
            severity = 'MEDIUM'
        elif count >= self.low:
            severity = 'LOW'

        anomaly = {
            'district': str(district),
            'event_count': int(count),
            'window_start': window_start,
            'window_end': window_end,
            'severity': severity,
            'threshold_value': int(self.threshold),
        }
        self.emit([anomaly])
