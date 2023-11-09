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
        Properties properties = Configs.subProperties(CommonConfig.SINK_KAFKA_PROPERTIES_PREFIX);
        properties.setProperty("bootstrap.servers", Configs.get(CommonConfig.KAFKA_OUTPUT_BOOTSTRAP_SERVERS));

        properties.put("acks", Configs.get(CommonConfig.KAFKA_OUTPUT_ACKS));
        properties.put("retries", Configs.get(CommonConfig.KAFKA_OUTPUT_RETRIES));
        properties.put("linger.ms", Configs.get(CommonConfig.KAFKA_OUTPUT_LINGER_MS));
        properties.put("request.timeout.ms", Configs.get(CommonConfig.KAFKA_OUTPUT_REQUEST_TIMEOUT_MS));
        properties.put("batch.size", Configs.get(CommonConfig.KAFKA_OUTPUT_BATCH_SIZE));
        properties.put("buffer.memory", Configs.get(CommonConfig.KAFKA_OUTPUT_BUFFER_MEMORY));
        properties.put("max.request.size", Configs.get(CommonConfig.KAFKA_OUTPUT_MAX_REQUEST_SIZE));
        properties.put("compression.type", Configs.get(CommonConfig.KAFKA_OUTPUT_COMPRESSION_TYPE));

        if (Configs.get(CommonConfig.OUTPUT_SASL_JAAS_CONFIG_FLAG) == 1) {
            properties.put("security.protocol", Configs.get(CommonConfig.KAFKA_OUTPUT_SECURITY_PROTOCOL));
            properties.put("sasl.mechanism", Configs.get(CommonConfig.KAFKA_OUTPUT_SASL_MECHANISM));
            properties.put("sasl.jaas.config", Configs.get(CommonConfig.KAFKA_OUTPUT_SASL_JAAS_CONFIG));
        }
        return properties;
    }

    public static Properties getDefaultKafkaSourceProperty() {
        Properties properties = Configs.subProperties(CommonConfig.SOURCE_KAFKA_PROPERTIES_PREFIX);
        properties.setProperty("bootstrap.servers", Configs.get(CommonConfig.KAFKA_INPUT_BOOTSTRAP_SERVERS));
        properties.put("session.timeout.ms", Configs.get(CommonConfig.KAFKA_INPUT_SESSION_TIMEOUT_MS));
        properties.put("max.poll.records", Configs.get(CommonConfig.KAFKA_INPUT_MAX_POLL_RECORDS));
        properties.put("max.partition.fetch.bytes", Configs.get(CommonConfig.KAFKA_INPUT_MAX_PARTITION_FETCH_BYTES));
        properties.put("partition.discovery.interval.ms", Configs.get(CommonConfig.KAFKA_INPUT_PARTITION_DISCOVERY_INTERVAL_MS));

        if (Configs.get(CommonConfig.INPUT_SASL_JAAS_CONFIG_FLAG) == 1) {
            properties.put("security.protocol", Configs.get(CommonConfig.KAFKA_INPUT_SECURITY_PROTOCOL));
            properties.put("sasl.mechanism", Configs.get(CommonConfig.KAFKA_INPUT_SASL_MECHANISM));
            properties.put("sasl.jaas.config", Configs.get(CommonConfig.KAFKA_INPUT_SASL_JAAS_CONFIG));
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
