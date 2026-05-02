package com.project;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

public class TopologyConfig {
    public final String topologyName;
    public final String kafkaBootstrapServers;
    public final String kafkaTopic;
    public final String postgresHost;
    public final int postgresPort;
    public final String postgresDatabase;
    public final String postgresUser;
    public final String postgresPassword;
    public final String mongoHost;
    public final int mongoPort;
    public final String mongoDatabase;
    public final String mongoAlertLogsCollection;
    public final int windowSizeSeconds;
    public final int slideIntervalSeconds;
    public final int anomalyThreshold;
    public final int lowMin;
    public final int mediumMin;
    public final int highMin;
    public final String kafkaGroupId;
    private final Map<String, Object> yamlConfig;

    private TopologyConfig() {
        this.yamlConfig = loadYamlConfig();
        this.topologyName = env("TOPOLOGY_NAME", yamlString("storm.topology_name", "crime-alert-topology"));
        this.kafkaBootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", yamlString("kafka.bootstrap_servers", "kafka:9092"));
        this.kafkaTopic = env("KAFKA_TOPIC_CRIME_EVENTS", yamlString("kafka.topic_crime_events", "crime-events"));
        this.postgresHost = env("POSTGRES_HOST", yamlString("postgres.host", "postgres"));
        this.postgresPort = envInt("POSTGRES_PORT", yamlInt("postgres.port", 5432));
        this.postgresDatabase = env("POSTGRES_DB", yamlString("postgres.database", "crime_analytics"));
        this.postgresUser = env("POSTGRES_USER", yamlString("postgres.user", "crime_user"));
        this.postgresPassword = env("POSTGRES_PASSWORD", yamlString("postgres.password", "crime_password"));
        this.mongoHost = env("MONGODB_HOST", yamlString("mongodb.host", "mongodb"));
        this.mongoPort = envInt("MONGODB_PORT", yamlInt("mongodb.port", 27017));
        this.mongoDatabase = env("MONGODB_DB", yamlString("mongodb.database", "crime_analytics"));
        this.mongoAlertLogsCollection = env("MONGODB_ALERT_LOGS_COLLECTION", yamlString("mongodb.alert_logs_collection", "alert_logs"));
        this.windowSizeSeconds = envInt("STORM_WINDOW_SIZE_SECONDS", yamlInt("storm.window_size_seconds", 300));
        this.slideIntervalSeconds = envInt("STORM_SLIDE_INTERVAL_SECONDS", yamlInt("storm.slide_interval_seconds", 60));
        this.anomalyThreshold = envInt("STORM_ANOMALY_THRESHOLD", yamlInt("storm.anomaly_threshold", 25));
        this.lowMin = envInt("STORM_SEVERITY_LOW_MIN", yamlInt("storm.severity.low_min", 25));
        this.mediumMin = envInt("STORM_SEVERITY_MEDIUM_MIN", yamlInt("storm.severity.medium_min", 40));
        this.highMin = envInt("STORM_SEVERITY_HIGH_MIN", yamlInt("storm.severity.high_min", 60));
        this.kafkaGroupId = env("KAFKA_GROUP_ID", "crime-alert-topology");
    }

    public static TopologyConfig load() {
        return new TopologyConfig();
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }

    private static int envInt(String name, int defaultValue) {
        try {
            return Integer.parseInt(env(name, String.valueOf(defaultValue)));
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadYamlConfig() {
        String configPath = env("CONFIG_PATH", "config/config.yaml");
        File file = new File(configPath);
        if (!file.exists()) {
            file = new File("/app/config/config.yaml");
        }
        if (!file.exists()) {
            return null;
        }
        try (FileInputStream stream = new FileInputStream(file)) {
            Object loaded = new Yaml().load(stream);
            if (loaded instanceof Map) {
                return (Map<String, Object>) loaded;
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private String yamlString(String path, String defaultValue) {
        Object value = yamlValue(path);
        return value == null || String.valueOf(value).trim().isEmpty() ? defaultValue : String.valueOf(value).trim();
    }

    private int yamlInt(String path, int defaultValue) {
        Object value = yamlValue(path);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return value == null ? defaultValue : Integer.parseInt(String.valueOf(value).trim());
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    @SuppressWarnings("unchecked")
    private Object yamlValue(String path) {
        if (yamlConfig == null) {
            return null;
        }
        Object current = yamlConfig;
        String[] parts = path.split("\\.");
        for (String part : parts) {
            if (!(current instanceof Map)) {
                return null;
            }
            current = ((Map<String, Object>) current).get(part);
            if (current == null) {
                return null;
            }
        }
        return current;
    }
}
