package com.project.bolts;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class DistrictBolt extends BaseRichBolt {
    private org.apache.storm.task.OutputCollector collector;

    @Override
    public void prepare(java.util.Map stormConf, org.apache.storm.task.TopologyContext context, org.apache.storm.task.OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) tuple.getValueByField("data");
        String district = normalizeDistrict(data.get("district"));
        data.put("district", district);
        collector.emit(new Values(district, data));
        collector.ack(tuple);
    }

    private String normalizeDistrict(Object value) {
        String district = value == null ? "UNKNOWN" : String.valueOf(value).trim();
        if (district.isEmpty()) {
            return "UNKNOWN";
        }
        if (district.endsWith(".0")) {
            district = district.substring(0, district.length() - 2);
        }
        if (district.matches("^[0-9]+$")) {
            return String.format("%03d", Integer.parseInt(district));
        }
        return district;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("district", "data"));
    }
}
