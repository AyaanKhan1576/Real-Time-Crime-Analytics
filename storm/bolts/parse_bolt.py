"""
ParseBolt skeleton: deserialize JSON, validate required fields, drop malformed messages.
"""
from streamparse.bolt import Bolt
import json
import logging


class ParseBolt(Bolt):
    def process(self, tup):
        try:
            msg = tup.values[0]
            if isinstance(msg, bytes):
                msg = msg.decode('utf-8')
            data = json.loads(msg)

            # Validate required fields
            required = ["case_number", "date", "block", "primary_type", "district", "latitude", "longitude"]
            for f in required:
                if f not in data or data[f] is None:
                    raise ValueError(f"missing {f}")

            # Ensure district is string
            data["district"] = str(data.get("district") or "UNKNOWN")

            self.emit([data])
        except Exception as e:
            logging.warning(f"ParseBolt dropped malformed message: {e}")
            # drop malformed
