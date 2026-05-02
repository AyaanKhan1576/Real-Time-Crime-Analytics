package com.project.bolts;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            data.put("arrest", parseBoolean(data.get("arrest")));
            data.put("latitude", parseDouble(data.get("latitude")));
            data.put("longitude", parseDouble(data.get("longitude")));

            collector.emit(new Values(data));
            collector.ack(tuple);
        } catch (Exception ex) {
            LOG.warn("ParseBolt dropped malformed message", ex);
            collector.ack(tuple);
        }
    }

    private boolean hasRequiredFields(Map<String, Object> data) {
        return hasText(data, "case_number")
                && hasText(data, "date")
                && hasText(data, "block")
                && hasText(data, "primary_type")
                && data.containsKey("district")
                && data.containsKey("arrest")
                && data.get("arrest") != null
                && hasText(data, "latitude")
                && hasText(data, "longitude");
    }

    private boolean hasText(Map<String, Object> data, String fieldName) {
        Object value = data.get(fieldName);
        return value != null && !String.valueOf(value).trim().isEmpty();
    }

    private String normalizeDistrict(Object value) {
        String district = value == null ? "UNKNOWN" : String.valueOf(value).trim();
        return district.isEmpty() ? "UNKNOWN" : district;
    }

    private boolean parseBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        String text = String.valueOf(value).trim().toLowerCase();
        if (text.equals("true") || text.equals("1") || text.equals("y") || text.equals("yes") || text.equals("t")) {
            return true;
        }
        if (text.equals("false") || text.equals("0") || text.equals("n") || text.equals("no") || text.equals("f")) {
            return false;
        }
        throw new IllegalArgumentException("invalid arrest value");
    }

    private double parseDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(String.valueOf(value).trim());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }
}
