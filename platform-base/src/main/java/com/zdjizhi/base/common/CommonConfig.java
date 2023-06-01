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

    public static final String LOCATION_SEPARATOR = ".";

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

    public static final int STREAM_EXECUTION_ENVIRONMENT_PARALLELISM = CommonConfigurations.getIntProperty("stream.execution.environment.parallelism");
    public static final String STREAM_EXECUTION_JOB_NAME = CommonConfigurations.getStringProperty("stream.execution.job.name");

    public static final int SESSION_RECORD_COMPLETED_PARALLELISM = CommonConfigurations.getIntProperty("session.record.completed.parallelism");
    public static final String SESSION_RECORD_COMPLETED_TOPIC = CommonConfigurations.getStringProperty("session.record.completed.topic");
    public static final String SESSION_RECORD_COMPLETED_GROUP_ID = CommonConfigurations.getStringProperty("session.record.completed.group.id");

    public static final String KAFKA_INPUT_BOOTSTRAP_SERVERS = CommonConfigurations.getStringProperty("kafka.input.bootstrap.servers");

    public static final String CN_RECORD_TOPIC = CommonConfigurations.getStringProperty("cn.record.topic");
    public static final int CN_RECORD_PARALLELISM = CommonConfigurations.getIntProperty("cn.record.parallelism");

    public static final String KAFKA_OUTPUT_BOOTSTRAP_SERVERS = CommonConfigurations.getStringProperty("kafka.output.bootstrap.servers");
    public static final String ZOOKEEPER_SERVERS = CommonConfigurations.getStringProperty("zookeeper.servers");

    public static final int DATA_CENTER_ID_NUM = CommonConfigurations.getIntProperty("data.center.id.num");

    public static final String ES_HOST = CommonConfigurations.getStringProperty("es.host");
    public static final int ES_SINK_PARALLELISM=CommonConfigurations.getIntProperty("es.sink.parallelism");

    public static final String SASL_JAAS_CONFIG_USER = CommonConfigurations.getStringProperty("sasl.jaas.config.user");
    public static final String SASL_JAAS_CONFIG_PASSWORD = CommonConfigurations.getStringProperty("sasl.jaas.config.password");

    public static final int INPUT_SASL_JAAS_CONFIG_FLAG = CommonConfigurations.getIntProperty("input.sasl.jaas.config.flag");
    public static final int OUTPUT_SASL_JAAS_CONFIG_FLAG = CommonConfigurations.getIntProperty("output.sasl.jaas.config.flag");

    public static final String FLINK_ENABLE_CHECKPOINT_FLAG = CommonConfigurations.getStringProperty("flink.enable.checkpoint.flag");
    public static final int FLINK_ENABLE_CHECKPOINT_TIME = CommonConfigurations.getIntProperty("flink.enable.checkpoint.time");
    public static final int FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS = CommonConfigurations.getIntProperty("flink.min.pause.between.checkpoints");
    public static final int FLINK_CHECKPOINT_TIMEOUT = CommonConfigurations.getIntProperty("flink.checkpoint.timeout");
    public static final int FLINK_TOLERABLE_CHECKPOINT_FAILURE_NUMBER = CommonConfigurations.getIntProperty("flink.tolerable.checkpoint.failure.number");
    public static final int FLINK_MAX_CONCURRENT_CHECKPOINT = CommonConfigurations.getIntProperty("flink.max.concurrent.checkpoint");

    public static final String KAFKA_PRODUCER_RETRIES = CommonConfigurations.getStringProperty("kafka.producer.retries");
    public static final String KAFKA_PRODUCER_LINGER_MS = CommonConfigurations.getStringProperty("kafka.producer.linger.ms");
    public static final String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS = CommonConfigurations.getStringProperty("kafka.producer.request.timeout.ms");
    public static final String KAFKA_PRODUCER_BATCH_SIZE = CommonConfigurations.getStringProperty("kafka.producer.batch.size");
    public static final String KAFKA_PRODUCER_BUFFER_MEMORY = CommonConfigurations.getStringProperty("kafka.producer.buffer.memory");
    public static final String KAFKA_PRODUCER_MAX_REQUEST_SIZE = CommonConfigurations.getStringProperty("kafka.producer.max.request.size");
    public static final String KAFKA_PRODUCER_COMPRESSION_TYPE = CommonConfigurations.getStringProperty("kafka.producer.compression.type");

    public static final int APP_LABEL_IS_THIRD = CommonConfigurations.getIntProperty("app.label.is.third");

    public static final int HTTP_POOL_MAX_CONNECTION = CommonConfigurations.getIntProperty("http.pool.max.connection");
    public static final int HTTP_POOL_MAX_PER_ROUTE = CommonConfigurations.getIntProperty("http.pool.max.per.route");
    public static final int HTTP_POOL_REQUEST_TIMEOUT = CommonConfigurations.getIntProperty("http.pool.request.timeout");
    public static final int HTTP_POOL_CONNECT_TIMEOUT = CommonConfigurations.getIntProperty("http.pool.connect.timeout");
    public static final int HTTP_POOL_RESPONSE_TIMEOUT = CommonConfigurations.getIntProperty("http.pool.response.timeout");

    public static final String NACOS_SERVER_ADDR = CommonConfigurations.getStringProperty("nacos.server.addr");
    public static final String NACOS_USERNAME = CommonConfigurations.getStringProperty("nacos.username");
    public static final String NACOS_PASSWORD = CommonConfigurations.getStringProperty("nacos.password");
    public static final String NACOS_NAMESPACE = CommonConfigurations.getStringProperty("nacos.namespace", "");
    public static final String NACOS_DATA_ID = CommonConfigurations.getStringProperty("nacos.data.id");
    public static final String NACOS_GROUP = CommonConfigurations.getStringProperty("nacos.group");
    public static final int NACOS_READ_TIMEOUT = CommonConfigurations.getIntProperty("nacos.read.timeout");
    public static final String HOS_TOKEN = CommonConfigurations.getStringProperty("hos.token");
    public static final String CLUSTER_OR_SINGLE = CommonConfigurations.getStringProperty("cluster.or.single");
    public static final String HDFS_URI_NS1 = CommonConfigurations.getStringProperty("hdfs.uri.nn1");
    public static final String HDFS_URI_NS2 = CommonConfigurations.getStringProperty("hdfs.uri.nn2");
    public static final String HDFS_PATH = CommonConfigurations.getStringProperty("hdfs.path");
    public static final String HDFS_USER = CommonConfigurations.getStringProperty("hdfs.user");
    public static final String DOWNLOAD_PATH = CommonConfigurations.getStringProperty("download.path");
    public static final int KNOWLEDGE_FILE_CHECK_NUMBER = CommonConfigurations.getIntProperty("knowledge.file.check.number");



}
