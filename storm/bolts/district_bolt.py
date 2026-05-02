"""
DistrictBolt skeleton: route/emit events grouped by `district`.
"""
from streamparse.bolt import Bolt


def normalize_district(value):
    district = str(value or "").strip()
    if not district:
        return "UNKNOWN"
    if district.endswith(".0"):
        district = district[:-2]
    return district.zfill(3) if district.isdigit() else district


class DistrictBolt(Bolt):
    def process(self, tup):
        data = tup.values[0]
        district = normalize_district(data.get("district"))
        data["district"] = district
        # emit with district key for downstream grouping
        self.emit([district, data])
