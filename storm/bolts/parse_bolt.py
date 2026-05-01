"""
ParseBolt skeleton: deserialize JSON, validate required fields, drop malformed messages.
"""
from streamparse.bolt import Bolt
import json
import logging
from datetime import datetime
import os
import sys


def _load_cfg(config_path):
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from config.config_loader import load_config

    return load_config(config_path)


class ParseBolt(Bolt):
    def initialize(self, conf, ctx):
        cfg = _load_cfg(conf.get("config_path", "config/config.yaml"))
        mongo_cfg = cfg.get("mongodb", {})
        self.enable_raw_event_logging = bool(mongo_cfg.get("enable_raw_event_logging", False))
        self.raw_coll = None
        if self.enable_raw_event_logging:
            try:
                from pymongo import MongoClient

                client = MongoClient(mongo_cfg.get("host", "mongodb"), int(mongo_cfg.get("port", 27017)))
                db = client[mongo_cfg.get("database", "crime_analytics")]
                self.raw_coll = db[mongo_cfg.get("raw_events_collection", "raw_events")]
            except Exception as exc:
                logging.warning("ParseBolt raw event logging disabled due to Mongo init error: %s", exc)
                self.enable_raw_event_logging = False

    def process(self, tup):
        try:
            msg = tup.values[0]
            if isinstance(msg, bytes):
                msg = msg.decode('utf-8')
            data = json.loads(msg)

            # Validate required fields
            required = ["case_number", "date", "block", "primary_type", "district", "arrest", "latitude", "longitude"]
            for f in required:
                if f not in data or data[f] is None:
                    raise ValueError(f"missing {f}")

            # Ensure district is string
            data["district"] = str(data.get("district") or "UNKNOWN").strip() or "UNKNOWN"
            data["arrest"] = bool(data.get("arrest"))
            data["latitude"] = float(data.get("latitude"))
            data["longitude"] = float(data.get("longitude"))

            # Optional raw event logging with strict schema fields.
            if self.enable_raw_event_logging and self.raw_coll is not None:
                raw_doc = {
                    "event_id": f"{data['case_number']}_{data['date']}",
                    "case_number": data["case_number"],
                    "date": data["date"],
                    "block": data["block"],
                    "primary_type": data["primary_type"],
                    "district": data["district"],
                    "arrest": bool(data.get("arrest", False)),
                    "latitude": data.get("latitude"),
                    "longitude": data.get("longitude"),
                    "ingested_at": datetime.utcnow(),
                }
                self.raw_coll.insert_one(raw_doc)

            self.emit([data])
        except Exception as e:
            logging.warning(f"ParseBolt dropped malformed message: {e}")
            # drop malformed
