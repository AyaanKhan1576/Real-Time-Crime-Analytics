"""
WindowBolt skeleton: maintain sliding window counts per district.
This is a simplified in-memory sliding window for local testing; in production
use durable state or Storm's windowing features.
"""
from streamparse.bolt import Bolt
from collections import defaultdict, deque
import time


class WindowBolt(Bolt):
    def initialize(self, conf, ctx):
        self.window_size = int(conf.get('window_size_seconds', 300))
        self.slide = int(conf.get('slide_interval_seconds', 60))
        self.buckets = defaultdict(deque)  # district -> deque of (timestamp)

    def process(self, tup):
        district, data = tup.values[0], tup.values[1]
        now = int(time.time())
        dq = self.buckets[district]
        dq.append(now)

        # Remove old
        while dq and dq[0] < now - self.window_size:
            dq.popleft()

        # Emit current count at slide intervals (simplified)
        count = len(dq)
        window_start = now - self.window_size
        window_end = now
        self.emit([district, count, window_start, window_end])
