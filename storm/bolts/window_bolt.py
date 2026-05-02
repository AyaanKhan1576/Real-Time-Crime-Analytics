"""
WindowBolt skeleton: maintain sliding window counts per district.
This is a simplified in-memory sliding window for local testing; in production
use durable state or Storm's windowing features.
"""
from streamparse.bolt import Bolt
from collections import defaultdict, deque
import time
import os
import sys
from datetime import datetime


def _load_cfg(config_path):
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from config.config_loader import load_config

    return load_config(config_path)


class WindowBolt(Bolt):
    def initialize(self, conf, ctx):
        cfg = _load_cfg(conf.get("config_path", "config/config.yaml"))
        storm_cfg = cfg.get("storm", {})
        self.window_size = int(storm_cfg.get("window_size_seconds", 300))
        self.slide = int(storm_cfg.get("slide_interval_seconds", 60))
        self.buckets = defaultdict(deque)  # district -> deque of (timestamp)
        self.last_emit = defaultdict(int)

    def process(self, tup):
        district, data = tup.values[0], tup.values[1]
        now = self._event_timestamp(data)
        dq = self.buckets[district]
        dq.append(now)

        # Remove old
        while dq and dq[0] < now - self.window_size:
            dq.popleft()

        # Emit at slide interval per district.
        if self.last_emit[district] and now - self.last_emit[district] < self.slide:
            return

        count = len(dq)
        window_start = now - self.window_size
        window_end = now
        self.last_emit[district] = now
        self.emit([district, count, window_start, window_end])

    def _event_timestamp(self, data):
        value = data.get("date") if isinstance(data, dict) else None
        if value:
            text = str(value).strip()
            for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    return int(datetime.strptime(text, fmt).timestamp())
                except ValueError:
                    continue
        return int(time.time())
