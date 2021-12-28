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

    private static Properties getKafkaSourceProperty() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", CommonConfig.KAFKA_INPUT_BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", CommonConfig.KAFKA_GROUP_ID);
        updateSasl(properties);
        return properties;
    }

    private static void updateSasl(Properties properties) {
        if (CommonConfig.SASL_JAAS_CONFIG_FLAG == 1) {
            properties.put("security.protocol", "SASL_PLAINTEXT");
            properties.put("sasl.mechanism", "PLAIN");
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + CommonConfig.SASL_JAAS_CONFIG_USER + "\" password=\"" + CommonConfig.SASL_JAAS_CONFIG_PASSWORD + "\";");
        }
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(),
                getKafkaSinkProperty(),
                Optional.empty()
        );
    }

    public static FlinkKafkaConsumer<String> createSource(String topic){
        return createSource(topic,getKafkaSourceProperty());
    }

    public static FlinkKafkaConsumer<String> createSource(String topic,Properties properties){
        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties);
    }

}
