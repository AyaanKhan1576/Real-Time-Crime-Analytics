package com.project;

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

    private TopologyConfig() {
        this.topologyName = env("TOPOLOGY_NAME", "crime-alert-topology");
        this.kafkaBootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        this.kafkaTopic = env("KAFKA_TOPIC_CRIME_EVENTS", "crime-events");
        this.postgresHost = env("POSTGRES_HOST", "postgres");
        this.postgresPort = envInt("POSTGRES_PORT", 5432);
        this.postgresDatabase = env("POSTGRES_DB", "crime_analytics");
        this.postgresUser = env("POSTGRES_USER", "crime_user");
        this.postgresPassword = env("POSTGRES_PASSWORD", "crime_password");
        this.mongoHost = env("MONGODB_HOST", "mongodb");
        this.mongoPort = envInt("MONGODB_PORT", 27017);
        this.mongoDatabase = env("MONGODB_DB", "crime_analytics");
        this.mongoAlertLogsCollection = env("MONGODB_ALERT_LOGS_COLLECTION", "alert_logs");
        this.windowSizeSeconds = envInt("STORM_WINDOW_SIZE_SECONDS", 300);
        this.slideIntervalSeconds = envInt("STORM_SLIDE_INTERVAL_SECONDS", 60);
        this.anomalyThreshold = envInt("STORM_ANOMALY_THRESHOLD", 25);
        this.lowMin = envInt("STORM_SEVERITY_LOW_MIN", 25);
        this.mediumMin = envInt("STORM_SEVERITY_MEDIUM_MIN", 40);
        this.highMin = envInt("STORM_SEVERITY_HIGH_MIN", 60);
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
}