package com.project.bolts;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class AnomalyBolt extends BaseRichBolt {
    private final int threshold;
    private final int lowMin;
    private final int mediumMin;
    private final int highMin;
    private org.apache.storm.task.OutputCollector collector;

    public AnomalyBolt(int threshold, int lowMin, int mediumMin, int highMin) {
        this.threshold = threshold;
        this.lowMin = lowMin;
        this.mediumMin = mediumMin;
        this.highMin = highMin;
    }

    @Override
    public void prepare(java.util.Map stormConf, TopologyContext context, org.apache.storm.task.OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String district = tuple.getStringByField("district");
        int count = tuple.getIntegerByField("count");
        long windowStart = tuple.getLongByField("window_start");
        long windowEnd = tuple.getLongByField("window_end");

        String severity = null;
        if (count >= highMin) {
            severity = "HIGH";
        } else if (count >= mediumMin) {
            severity = "MEDIUM";
        } else if (count >= lowMin) {
            severity = "LOW";
        }

        Map<String, Object> anomaly = new HashMap<String, Object>();
        anomaly.put("district", district);
        anomaly.put("event_count", count);
        anomaly.put("window_start", windowStart);
        anomaly.put("window_end", windowEnd);
        anomaly.put("severity", severity);
        anomaly.put("threshold_value", threshold);
        collector.emit(new Values(anomaly));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("anomaly"));
    }
}