"""
Kafka spout for the Storm speed layer.

This emits raw JSON strings from the crime-events topic so the downstream
bolts can validate and normalize the shared data contract.
"""
import json
import logging
import os
import sys
from time import sleep

from kafka import KafkaConsumer

try:
    from streamparse.spout import Spout
except Exception:
    class Spout:
        @classmethod
        def spec(cls, **kwargs):
            return {"component": cls.__name__, "kwargs": kwargs}

        def initialize(self, conf, ctx):
            return None

        def emit(self, values):
            try:
                emitted = getattr(self, "_emitted", None)
                if emitted is None:
                    self._emitted = []
                    emitted = self._emitted
                emitted.append(values)
            except Exception:
                pass


def _load_cfg(config_path):
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    from config.config_loader import load_config

    return load_config(config_path)


class CrimeEventSpout(Spout):
    def initialize(self, conf, ctx):
        cfg = _load_cfg(conf.get("config_path", "config/config.yaml"))
        kafka_cfg = cfg.get("kafka", {})
        self.topic = kafka_cfg.get("topic_crime_events", "crime-events")
        self.bootstrap = kafka_cfg.get("bootstrap_servers", "kafka:9092")
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap,
            value_deserializer=lambda raw: raw.decode("utf-8") if raw is not None else "",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="crime-alert-topology",
            consumer_timeout_ms=1000,
        )

    def next_tuple(self):
        try:
            records = self.consumer.poll(timeout_ms=100, max_records=10)
            if not records:
                sleep(0.05)
                return

            for _, batch in records.items():
                for record in batch:
                    payload = record.value
                    if isinstance(payload, bytes):
                        payload = payload.decode("utf-8")
                    self.emit([payload])
        except Exception as exc:
            logging.warning("CrimeEventSpout poll failed: %s", exc)
            sleep(0.5)

    def ack(self, tup_id):
        return None

    def fail(self, tup_id):
        return None
