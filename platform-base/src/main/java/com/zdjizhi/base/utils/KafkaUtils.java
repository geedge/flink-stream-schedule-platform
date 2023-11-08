package com.zdjizhi.base.utils;

import com.zdjizhi.base.common.CommonConfig;
import com.zdjizhi.base.config.Configs;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Optional;
import java.util.Properties;

public class KafkaUtils {

    private static Properties getKafkaSinkProperty() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Configs.get(CommonConfig.KAFKA_OUTPUT_BOOTSTRAP_SERVERS));

        properties.put("acks", "1");
        properties.put("retries", Configs.get(CommonConfig.KAFKA_PRODUCER_RETRIES));
        properties.put("linger.ms", Configs.get(CommonConfig.KAFKA_PRODUCER_LINGER_MS));
        properties.put("request.timeout.ms", Configs.get(CommonConfig.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS));
        properties.put("batch.size", Configs.get(CommonConfig.KAFKA_PRODUCER_BATCH_SIZE));
        properties.put("buffer.memory", Configs.get(CommonConfig.KAFKA_PRODUCER_BUFFER_MEMORY));
        properties.put("max.request.size", Configs.get(CommonConfig.KAFKA_PRODUCER_MAX_REQUEST_SIZE));
        properties.put("compression.type", Configs.get(CommonConfig.KAFKA_PRODUCER_COMPRESSION_TYPE));

        if (Configs.get(CommonConfig.OUTPUT_SASL_JAAS_CONFIG_FLAG) == 1) {
            KafkaUtils.updateSasl(properties);
        }
        return properties;
    }

    private static void updateSasl(Properties properties) {
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + Configs.get(CommonConfig.SASL_JAAS_CONFIG_USER) + "\" password=\"" + Configs.get(CommonConfig.SASL_JAAS_CONFIG_PASSWORD) + "\";");
    }

    public static Properties getDefaultKafkaSourceProperty() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Configs.get(CommonConfig.KAFKA_INPUT_BOOTSTRAP_SERVERS));
        properties.put("session.timeout.ms", 60000);
        properties.put("max.poll.records", 5000);
        properties.put("max.partition.fetch.bytes", 31457280);
        properties.put("partition.discovery.interval.ms", "10000");

        if (Configs.get(CommonConfig.INPUT_SASL_JAAS_CONFIG_FLAG) == 1) {
            KafkaUtils.updateSasl(properties);
        }
        return properties;
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                topic,
                new SimpleStringSchema(),
                getKafkaSinkProperty(),
                Optional.empty()
        );
        kafkaProducer.setLogFailuresOnly(true);
        return kafkaProducer;
    }

    public static FlinkKafkaConsumer<String> createSource(String topic, Properties properties) {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        return kafkaConsumer;
    }
}
