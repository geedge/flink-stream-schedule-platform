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
        updateSasl(properties);
        return properties;
    }

    public static void updateSasl(Properties properties) {
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + CommonConfig.SASL_JAAS_CONFIG_USER + "\" password=\"" + CommonConfig.SASL_JAAS_CONFIG_PASSWORD + "\";");

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
