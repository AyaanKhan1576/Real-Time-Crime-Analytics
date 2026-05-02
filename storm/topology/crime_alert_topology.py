"""
Topology skeleton (Python) for streamparse or illustrative purposes.
Replace with actual streamparse/Java topology when implementing.
"""
from streamparse import Topology
from storm.bolts.parse_bolt import ParseBolt
from storm.bolts.district_bolt import DistrictBolt
from storm.bolts.window_bolt import WindowBolt
from storm.bolts.anomaly_bolt import AnomalyBolt
from storm.bolts.alert_bolt import AlertBolt


class CrimeAlertTopology(Topology):
    parse = ParseBolt.spec()
    district = DistrictBolt.spec(inputs=[parse])
    window = WindowBolt.spec(inputs=[district])
    anomaly = AnomalyBolt.spec(inputs=[window])
    alert = AlertBolt.spec(inputs=[anomaly])
