"""
AlertBolt skeleton: persist anomalies to PostgreSQL and MongoDB.
Ensure alert_id is deterministic: district_windowstart_windowend
"""
from streamparse.bolt import Bolt
import psycopg2
from psycopg2.extras import DictCursor
from pymongo import MongoClient
from datetime import datetime
import logging
import os
import sys


def _load_cfg(config_path):
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from config.config_loader import load_config

    return load_config(config_path)


class AlertBolt(Bolt):
    def initialize(self, conf, ctx):
        cfg_path = conf.get("config_path", "config/config.yaml")
        cfg = _load_cfg(cfg_path)

        pg = cfg["postgres"]
        self.pg_conn = psycopg2.connect(
            host=pg["host"],
            port=pg["port"],
            dbname=pg["database"],
            user=pg["user"],
            password=pg["password"],
            cursor_factory=DictCursor,
        )
        self.pg_conn.autocommit = False

        mv = cfg["mongodb"]
        self.mongo = MongoClient(mv["host"], int(mv["port"]))
        self.db = self.mongo[mv["database"]]
        self.alerts_coll = self.db[mv["alert_logs_collection"]]

    def process(self, tup):
        anomaly = tup.values[0]
        district = str(anomaly["district"]) if anomaly.get("district") is not None else "UNKNOWN"
        window_start_ts = int(anomaly["window_start"])
        window_end_ts = int(anomaly["window_end"])
        event_count = int(anomaly["event_count"])
        threshold_value = int(anomaly.get("threshold_value", 25))
        severity = anomaly.get("severity")

        ws = datetime.utcfromtimestamp(window_start_ts).strftime('%Y%m%dT%H%M%S')
        we = datetime.utcfromtimestamp(window_end_ts).strftime('%Y%m%dT%H%M%S')
        alert_id = f"{district}_{ws}_{we}"

        # Always upsert real-time district counts for dashboard live view.
        cur = self.pg_conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO realtime_district_counts (district, window_start, window_end, event_count, updated_at)
                VALUES (%s, to_timestamp(%s), to_timestamp(%s), %s, CURRENT_TIMESTAMP)
                ON CONFLICT (district, window_start, window_end)
                DO UPDATE SET
                    event_count = EXCLUDED.event_count,
                    updated_at = CURRENT_TIMESTAMP;
                """,
                (district, window_start_ts, window_end_ts, event_count),
            )

            if severity is not None:
                cur.execute(
                    """
                INSERT INTO alerts (alert_id, district, alert_timestamp, window_start, window_end, event_count, threshold_value, severity, message, source)
                VALUES (%s, %s, CURRENT_TIMESTAMP, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, 'storm')
                ON CONFLICT (alert_id) DO NOTHING;
                """,
                    (
                        alert_id,
                        district,
                        window_start_ts,
                        window_end_ts,
                        event_count,
                        threshold_value,
                        severity,
                        f"Crime spike detected in district {district}",
                    ),
                )

            self.pg_conn.commit()
        except Exception as exc:
            self.pg_conn.rollback()
            logging.exception("PostgreSQL write failed in AlertBolt: %s", exc)
        finally:
            cur.close()

        # Write alert logs only when this tuple is an actual alert.
        if severity is not None:
            doc = {
                "alert_id": alert_id,
                "district": district,
                "timestamp": datetime.utcnow(),
                "window_start": datetime.utcfromtimestamp(window_start_ts),
                "window_end": datetime.utcfromtimestamp(window_end_ts),
                "event_count": event_count,
                "threshold": threshold_value,
                "severity": severity,
                "message": f"Crime spike detected in district {district}",
                "source": "storm",
            }
            try:
                self.alerts_coll.update_one({"alert_id": alert_id}, {"$setOnInsert": doc}, upsert=True)
            except Exception as exc:
                logging.warning("Mongo alert write failed for %s: %s", alert_id, exc)
