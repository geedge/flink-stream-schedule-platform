package com.zdjizhi.base.utils;

import com.zdjizhi.base.common.CommonConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Optional;
import java.util.Properties;

public class KafkaUtils {

    private static Properties getKafkaSinkProperty() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", CommonConfig.KAFKA_OUTPUT_BOOTSTRAP_SERVERS);

        properties.put("acks", "1");
        properties.put("retries", CommonConfig.KAFKA_PRODUCER_RETRIES);
        properties.put("linger.ms", CommonConfig.KAFKA_PRODUCER_LINGER_MS);
        properties.put("request.timeout.ms", CommonConfig.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS);
        properties.put("batch.size", CommonConfig.KAFKA_PRODUCER_BATCH_SIZE);
        properties.put("buffer.memory", CommonConfig.KAFKA_PRODUCER_BUFFER_MEMORY);
        properties.put("max.request.size", CommonConfig.KAFKA_PRODUCER_MAX_REQUEST_SIZE);
        properties.put("compression.type", "none");

        if (CommonConfig.OUTPUT_SASL_JAAS_CONFIG_FLAG == 1) {
            KafkaUtils.updateSasl(properties);
        }
        return properties;
    }

    private static void updateSasl(Properties properties) {
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + CommonConfig.SASL_JAAS_CONFIG_USER + "\" password=\"" + CommonConfig.SASL_JAAS_CONFIG_PASSWORD + "\";");
    }

    public static Properties getDefaultKafkaSourceProperty() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", CommonConfig.KAFKA_INPUT_BOOTSTRAP_SERVERS);
        properties.put("session.timeout.ms", 60000);
        properties.put("max.poll.records", 5000);
        properties.put("max.partition.fetch.bytes", 31457280);
        properties.put("partition.discovery.interval.ms", "10000");

        if (CommonConfig.INPUT_SASL_JAAS_CONFIG_FLAG == 1) {
            KafkaUtils.updateSasl(properties);
        }
        return properties;
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(),
                getKafkaSinkProperty(),
                Optional.empty()
        );
    }

    public static FlinkKafkaConsumer<String> createSource(String topic, Properties properties) {
        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties);
    }

}
