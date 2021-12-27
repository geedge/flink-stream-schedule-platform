package com.zdjizhi.base.common;

import com.zdjizhi.base.utils.CommonConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author wlh
 * @date 2021/1/6
 */
public class CommonConfig {

    private static CommonConfigurations configurations;
    private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);

    static {
        Properties propService;
        try {
            propService = new Properties();
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("common.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载common.properties配置文件失败");
            System.exit(1);
        }
    }

    public static final int STREAM_EXECUTION_ENVIRONMENT_PARALLELISM = configurations.getIntProperty("stream.execution.environment.parallelism");
    public static final String STREAM_EXECUTION_JOB_NAME = configurations.getStringProperty("stream.execution.job.name");

    public static final int KAFKA_INPUT_PARALLELISM = configurations.getIntProperty("kafka.input.parallelism");
    public static final String KAFKA_INPUT_TOPIC_NAME = configurations.getStringProperty("kafka.input.topic.name");
    public static final String KAFKA_INPUT_BOOTSTRAP_SERVERS = configurations.getStringProperty("kafka.input.bootstrap.servers");
    public static final String KAFKA_GROUP_ID = configurations.getStringProperty("kafka.input.group.id");

    public static final int KAFKA_OUTPUT_PARALLELISM = configurations.getIntProperty("kafka.output.parallelism");
    public static final String KAFKA_OUTPUT_TOPIC_NAME = configurations.getStringProperty("kafka.output.topic.name");
    public static final String KAFKA_OUTPUT_BOOTSTRAP_SERVERS = configurations.getStringProperty("kafka.output.bootstrap.servers");

    public static final String SASL_JAAS_CONFIG_USER = configurations.getStringProperty("sasl.jaas.config.user");
    public static final String SASL_JAAS_CONFIG_PASSWORD = configurations.getStringProperty("sasl.jaas.config.password");

    public static final int SASL_JAAS_CONFIG_FLAG = configurations.getIntProperty("sasl.jaas.config.flag");

}
