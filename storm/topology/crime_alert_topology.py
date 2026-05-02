"""
Streamparse topology for the streaming speed layer.
Kafka event spout feeds the Parse -> District -> Window -> Anomaly -> Alert chain.
"""
try:
    from streamparse import Topology
except Exception:
    class Topology:
        pass
from storm.spouts.kafka_spout import CrimeEventSpout
from storm.bolts.parse_bolt import ParseBolt
from storm.bolts.district_bolt import DistrictBolt
from storm.bolts.window_bolt import WindowBolt
from storm.bolts.anomaly_bolt import AnomalyBolt
from storm.bolts.alert_bolt import AlertBolt


class CrimeAlertTopology(Topology):
    events = CrimeEventSpout.spec()
    parse = ParseBolt.spec(inputs=[events])
    district = DistrictBolt.spec(inputs=[parse])
    window = WindowBolt.spec(inputs=[district])
    anomaly = AnomalyBolt.spec(inputs=[window])
    alert = AlertBolt.spec(inputs=[anomaly])
