"""
AnomalyBolt skeleton: compare counts against threshold and emit anomalies.
"""
from streamparse.bolt import Bolt
import yaml


class AnomalyBolt(Bolt):
    def initialize(self, conf, ctx):
        cfg_path = conf.get('config_path', 'config/config.yaml')
        with open(cfg_path, 'r') as f:
            cfg = yaml.safe_load(f)
        self.threshold = int(cfg.get('storm', {}).get('anomaly_threshold', 25))
        sev = cfg.get('storm', {}).get('severity', {})
        self.low = int(sev.get('low_min', 25))
        self.medium = int(sev.get('medium_min', 40))
        self.high = int(sev.get('high_min', 60))

    def process(self, tup):
        district, count, window_start, window_end = tup.values
        if count < self.low:
            return

        if count >= self.high:
            severity = 'HIGH'
        elif count >= self.medium:
            severity = 'MEDIUM'
        else:
            severity = 'LOW'

        anomaly = {
            'district': str(district),
            'event_count': int(count),
            'window_start': window_start,
            'window_end': window_end,
            'severity': severity
        }
        self.emit([anomaly])
