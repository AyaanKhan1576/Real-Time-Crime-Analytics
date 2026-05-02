"""
DistrictBolt skeleton: route/emit events grouped by `district`.
"""
from streamparse.bolt import Bolt


class DistrictBolt(Bolt):
    def process(self, tup):
        data = tup.values[0]
        district = str(data.get("district") or "UNKNOWN").strip() or "UNKNOWN"
        data["district"] = district
        # emit with district key for downstream grouping
        self.emit([district, data])
