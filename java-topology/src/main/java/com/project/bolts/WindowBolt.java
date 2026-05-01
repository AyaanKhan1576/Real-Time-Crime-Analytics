package com.project.bolts;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

public class WindowBolt extends BaseRichBolt {
    private final int windowSizeSeconds;
    private final int slideIntervalSeconds;
    private final Map<String, Deque<Long>> buckets = new HashMap<String, Deque<Long>>();
    private final Map<String, Long> lastEmit = new HashMap<String, Long>();
    private org.apache.storm.task.OutputCollector collector;

    public WindowBolt(int windowSizeSeconds, int slideIntervalSeconds) {
        this.windowSizeSeconds = windowSizeSeconds;
        this.slideIntervalSeconds = slideIntervalSeconds;
    }

    @Override
    public void prepare(java.util.Map stormConf, TopologyContext context, org.apache.storm.task.OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String district = tuple.getStringByField("district");
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) tuple.getValueByField("data");
        long eventTs = eventTimestamp(data.get("date"));

        Deque<Long> bucket = buckets.containsKey(district) ? buckets.get(district) : new ArrayDeque<Long>();
        bucket.addLast(eventTs);
        while (!bucket.isEmpty() && bucket.peekFirst() < eventTs - windowSizeSeconds) {
            bucket.removeFirst();
        }
        buckets.put(district, bucket);

        Long lastEmitTs = lastEmit.get(district);
        if (lastEmitTs != null && eventTs - lastEmitTs < slideIntervalSeconds) {
            collector.ack(tuple);
            return;
        }

        lastEmit.put(district, eventTs);
        long windowStart = eventTs - windowSizeSeconds;
        long windowEnd = eventTs;
        collector.emit(new Values(district, bucket.size(), windowStart, windowEnd));
        collector.ack(tuple);
    }

    private long eventTimestamp(Object value) {
        String text = value == null ? null : String.valueOf(value).trim();
        if (text != null && !text.isEmpty()) {
            String[] patterns = new String[] {
                    "MM/dd/yyyy hh:mm:ss a",
                    "MM/dd/yyyy HH:mm:ss",
                    "yyyy-MM-dd'T'HH:mm:ss",
                    "yyyy-MM-dd HH:mm:ss"
            };
            for (String pattern : patterns) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.US);
                    sdf.setLenient(true);
                    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                    Date parsed = sdf.parse(text);
                    if (parsed != null) {
                        return parsed.getTime() / 1000L;
                    }
                } catch (ParseException ignored) {
                }
            }
        }
        return System.currentTimeMillis() / 1000L;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("district", "count", "window_start", "window_end"));
    }
}