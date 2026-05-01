package com.project.bolts;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static com.mongodb.client.model.Filters.eq;

public class AlertBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AlertBolt.class);
    private final String postgresHost;
    private final int postgresPort;
    private final String postgresDatabase;
    private final String postgresUser;
    private final String postgresPassword;
    private final String mongoHost;
    private final int mongoPort;
    private final String mongoDatabase;
    private final String mongoAlertLogsCollection;
    private Connection connection;
    private MongoClient mongoClient;
    private MongoCollection<Document> alertLogsCollection;

    public AlertBolt(String postgresHost, int postgresPort, String postgresDatabase, String postgresUser, String postgresPassword,
                     String mongoHost, int mongoPort, String mongoDatabase, String mongoAlertLogsCollection) {
        this.postgresHost = postgresHost;
        this.postgresPort = postgresPort;
        this.postgresDatabase = postgresDatabase;
        this.postgresUser = postgresUser;
        this.postgresPassword = postgresPassword;
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.mongoDatabase = mongoDatabase;
        this.mongoAlertLogsCollection = mongoAlertLogsCollection;
    }

    @Override
    public void prepare(java.util.Map stormConf, TopologyContext context, org.apache.storm.task.OutputCollector collector) {
        try {
            Class.forName("org.postgresql.Driver");
            String jdbcUrl = "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/" + postgresDatabase;
            connection = DriverManager.getConnection(jdbcUrl, postgresUser, postgresPassword);
            connection.setAutoCommit(false);

            mongoClient = MongoClients.create("mongodb://" + mongoHost + ":" + mongoPort);
            MongoDatabase db = mongoClient.getDatabase(mongoDatabase);
            alertLogsCollection = db.getCollection(mongoAlertLogsCollection);
        } catch (Exception ex) {
            throw new RuntimeException("AlertBolt initialization failed", ex);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        @SuppressWarnings("unchecked")
        Map<String, Object> anomaly = (Map<String, Object>) tuple.getValueByField("anomaly");
        String district = normalizeDistrict(anomaly.get("district"));
        long windowStartTs = ((Number) anomaly.get("window_start")).longValue();
        long windowEndTs = ((Number) anomaly.get("window_end")).longValue();
        int eventCount = ((Number) anomaly.get("event_count")).intValue();
        int thresholdValue = ((Number) anomaly.get("threshold_value")).intValue();
        String severity = anomaly.get("severity") == null ? null : String.valueOf(anomaly.get("severity"));

        Timestamp windowStart = new Timestamp(windowStartTs * 1000L);
        Timestamp windowEnd = new Timestamp(windowEndTs * 1000L);
        String alertId = buildAlertId(district, windowStartTs, windowEndTs);

        try (PreparedStatement countsStmt = connection.prepareStatement(
                "INSERT INTO realtime_district_counts (district, window_start, window_end, event_count, updated_at) " +
                "VALUES (?, to_timestamp(?), to_timestamp(?), ?, CURRENT_TIMESTAMP) " +
                "ON CONFLICT (district, window_start, window_end) DO UPDATE SET event_count = EXCLUDED.event_count, updated_at = CURRENT_TIMESTAMP")) {
            countsStmt.setString(1, district);
            countsStmt.setLong(2, windowStartTs);
            countsStmt.setLong(3, windowEndTs);
            countsStmt.setLong(4, eventCount);
            countsStmt.executeUpdate();

            if (severity != null) {
                try (PreparedStatement alertsStmt = connection.prepareStatement(
                        "INSERT INTO alerts (alert_id, district, alert_timestamp, window_start, window_end, event_count, threshold_value, severity, message, source) " +
                        "VALUES (?, ?, CURRENT_TIMESTAMP, to_timestamp(?), to_timestamp(?), ?, ?, ?, ?, 'storm') ON CONFLICT (alert_id) DO NOTHING")) {
                    alertsStmt.setString(1, alertId);
                    alertsStmt.setString(2, district);
                    alertsStmt.setLong(3, windowStartTs);
                    alertsStmt.setLong(4, windowEndTs);
                    alertsStmt.setLong(5, eventCount);
                    alertsStmt.setInt(6, thresholdValue);
                    alertsStmt.setString(7, severity);
                    alertsStmt.setString(8, "Crime spike detected in district " + district);
                    alertsStmt.executeUpdate();
                }
            }

            connection.commit();
        } catch (Exception ex) {
            try {
                connection.rollback();
            } catch (Exception rollbackEx) {
                LOG.warn("AlertBolt rollback failed", rollbackEx);
            }
            LOG.warn("PostgreSQL write failed in AlertBolt", ex);
        }

        if (severity != null) {
            Document doc = new Document("alert_id", alertId)
                    .append("district", district)
                    .append("timestamp", new Date())
                    .append("window_start", new Date(windowStart.getTime()))
                    .append("window_end", new Date(windowEnd.getTime()))
                    .append("event_count", eventCount)
                    .append("threshold", thresholdValue)
                    .append("severity", severity)
                    .append("message", "Crime spike detected in district " + district)
                    .append("source", "storm");
            try {
                alertLogsCollection.updateOne(eq("alert_id", alertId), new Document("$setOnInsert", doc), new UpdateOptions().upsert(true));
            } catch (Exception ex) {
                LOG.warn("Mongo alert write failed for {}", alertId, ex);
            }
        }
    }

    private String normalizeDistrict(Object value) {
        String district = value == null ? "UNKNOWN" : String.valueOf(value).trim();
        return district.isEmpty() ? "UNKNOWN" : district;
    }

    private String buildAlertId(String district, long windowStartTs, long windowEndTs) {
        return district + "_" + formatUtc(windowStartTs) + "_" + formatUtc(windowEndTs);
    }

    private String formatUtc(long epochSeconds) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd'T'HHmmss", Locale.US);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        return formatter.format(new Date(epochSeconds * 1000L));
    }

    @Override
    public void cleanup() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ignored) {
        }
        try {
            if (mongoClient != null) {
                mongoClient.close();
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}