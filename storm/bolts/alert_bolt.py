"""
AlertBolt skeleton: persist anomalies to PostgreSQL and MongoDB.
Ensure alert_id is deterministic: district_windowstart_windowend
"""
from streamparse.bolt import Bolt
import psycopg2
from pymongo import MongoClient
import yaml
from datetime import datetime


class AlertBolt(Bolt):
    def initialize(self, conf, ctx):
        cfg_path = conf.get('config_path', 'config/config.yaml')
        with open(cfg_path, 'r') as f:
            cfg = yaml.safe_load(f)

        pg = cfg['postgres']
        self.pg_conn = psycopg2.connect(host=pg['host'], port=pg['port'], dbname=pg['database'], user=pg['user'], password=pg['password'])
        mv = cfg['mongodb']
        self.mongo = MongoClient(mv['host'], int(mv['port']))
        self.db = self.mongo[mv['database']]
        self.alerts_coll = self.db[mv['alert_logs_collection']]
        self.enable_raw = mv.get('enable_raw_event_logging', False)

    def process(self, tup):
        anomaly = tup.values[0]
        district = anomaly['district']
        ws = datetime.utcfromtimestamp(anomaly['window_start']).strftime('%Y%m%dT%H%M%S')
        we = datetime.utcfromtimestamp(anomaly['window_end']).strftime('%Y%m%dT%H%M%S')
        alert_id = f"{district}_{ws}_{we}"

        # Insert into PostgreSQL alerts table (idempotent on alert_id)
        cur = self.pg_conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO alerts (alert_id, district, alert_timestamp, window_start, window_end, event_count, threshold_value, severity, message, source)
                VALUES (%s, %s, CURRENT_TIMESTAMP, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, 'storm')
                ON CONFLICT (alert_id) DO NOTHING;
                """,
                (alert_id, district, anomaly['window_start'], anomaly['window_end'], anomaly['event_count'], anomaly.get('threshold_value', None), anomaly['severity'], f"Crime spike detected in district {district}")
            )
            self.pg_conn.commit()
        except Exception:
            self.pg_conn.rollback()

        # Insert into MongoDB alert_logs (idempotent using alert_id unique index)
        doc = {
            'alert_id': alert_id,
            'district': district,
            'timestamp': datetime.utcnow(),
            'window_start': datetime.utcfromtimestamp(anomaly['window_start']),
            'window_end': datetime.utcfromtimestamp(anomaly['window_end']),
            'event_count': anomaly['event_count'],
            'threshold': anomaly.get('threshold_value', None),
            'severity': anomaly['severity'],
            'message': f"Crime spike detected in district {district}",
            'source': 'storm'
        }
        try:
            self.alerts_coll.insert_one(doc)
        except Exception:
            # duplicate key or other error - ignore for idempotency
            pass
