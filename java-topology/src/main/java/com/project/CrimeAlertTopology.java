package com.project;

import com.project.bolts.AlertBolt;
import com.project.bolts.AnomalyBolt;
import com.project.bolts.DistrictBolt;
import com.project.bolts.ParseBolt;
import com.project.bolts.WindowBolt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class CrimeAlertTopology {
    public static void main(String[] args) throws Exception {
        TopologyConfig cfg = TopologyConfig.load();
        TopologyBuilder builder = new TopologyBuilder();

        KafkaSpoutConfig<String, String> spoutConfig = KafkaSpoutConfig.builder(cfg.kafkaBootstrapServers, cfg.kafkaTopic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, cfg.kafkaGroupId)
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .build();

        builder.setSpout("crime-events", new KafkaSpout<>(spoutConfig), 1);
        builder.setBolt("parse-bolt", new ParseBolt(), 2).shuffleGrouping("crime-events");
        builder.setBolt("district-bolt", new DistrictBolt(), 2).shuffleGrouping("parse-bolt");
        builder.setBolt("window-bolt", new WindowBolt(cfg.windowSizeSeconds, cfg.slideIntervalSeconds), 2)
                .fieldsGrouping("district-bolt", new org.apache.storm.tuple.Fields("district"));
        builder.setBolt("anomaly-bolt", new AnomalyBolt(cfg.anomalyThreshold, cfg.lowMin, cfg.mediumMin, cfg.highMin), 2)
                .shuffleGrouping("window-bolt");
        builder.setBolt("alert-bolt", new AlertBolt(
            cfg.postgresHost,
            cfg.postgresPort,
            cfg.postgresDatabase,
            cfg.postgresUser,
            cfg.postgresPassword,
            cfg.mongoHost,
            cfg.mongoPort,
            cfg.mongoDatabase,
            cfg.mongoAlertLogsCollection), 1).shuffleGrouping("anomaly-bolt");

        Config stormConf = new Config();
        stormConf.setNumWorkers(2);
        stormConf.setMaxSpoutPending(200);
        stormConf.registerSerialization(LinkedHashMap.class);
        stormConf.registerSerialization(HashMap.class);
        stormConf.registerSerialization(ArrayList.class);
        stormConf.put("topology.name", cfg.topologyName);
        stormConf.put("postgres.host", cfg.postgresHost);
        stormConf.put("postgres.port", cfg.postgresPort);
        stormConf.put("postgres.database", cfg.postgresDatabase);
        stormConf.put("postgres.user", cfg.postgresUser);
        stormConf.put("postgres.password", cfg.postgresPassword);
        stormConf.put("mongodb.host", cfg.mongoHost);
        stormConf.put("mongodb.port", cfg.mongoPort);
        stormConf.put("mongodb.database", cfg.mongoDatabase);
        stormConf.put("mongodb.alert_logs_collection", cfg.mongoAlertLogsCollection);

        StormSubmitter.submitTopology(cfg.topologyName, stormConf, builder.createTopology());
    }
}