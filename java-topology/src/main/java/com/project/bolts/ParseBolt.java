package com.project.bolts;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ParseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ParseBolt.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private org.apache.storm.task.OutputCollector collector;

    @Override
    public void prepare(java.util.Map stormConf, org.apache.storm.task.TopologyContext context, org.apache.storm.task.OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Object raw = tuple.getValueByField("value");
            if (raw == null && tuple.size() > 0) {
                raw = tuple.getValue(0);
            }
            String json = raw == null ? null : String.valueOf(raw);
            if (json == null || json.trim().isEmpty()) {
                collector.ack(tuple);
                return;
            }

            Map<String, Object> data = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            if (!hasRequiredFields(data)) {
                LOG.warn("ParseBolt dropped malformed message: {}", json);
                collector.ack(tuple);
                return;
            }

            data.put("district", normalizeDistrict(data.get("district")));
            data.put("arrest", asBoolean(data.get("arrest")));
            data.put("latitude", asDouble(data.get("latitude")));
            data.put("longitude", asDouble(data.get("longitude")));

            collector.emit(new Values(data));
            collector.ack(tuple);
        } catch (Exception ex) {
            LOG.warn("ParseBolt dropped malformed message", ex);
            collector.ack(tuple);
        }
    }

    private boolean hasRequiredFields(Map<String, Object> data) {
        return data.get("case_number") != null
                && data.get("date") != null
                && data.get("block") != null
                && data.get("primary_type") != null;
    }

    private String normalizeDistrict(Object value) {
        String district = value == null ? "UNKNOWN" : String.valueOf(value).trim();
        return district.isEmpty() ? "UNKNOWN" : district;
    }

    private boolean asBoolean(Object value) {
        if (value == null) {
            return false;
        }
        String text = String.valueOf(value).trim().toLowerCase();
        return text.equals("true") || text.equals("1") || text.equals("y") || text.equals("yes") || text.equals("t");
    }

    private double asDouble(Object value) {
        try {
            if (value == null) {
                return 0.0d;
            }
            String text = String.valueOf(value).trim();
            if (text.isEmpty()) {
                return 0.0d;
            }
            return Double.parseDouble(text);
        } catch (Exception ex) {
            return 0.0d;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }
}