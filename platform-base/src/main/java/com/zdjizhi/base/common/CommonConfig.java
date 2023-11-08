package com.zdjizhi.base.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author wlh
 * @date 2021/1/6
 */
public class CommonConfig {

    public static final ConfigOption<Integer> STREAM_EXECUTION_ENVIRONMENT_PARALLELISM = ConfigOptions.key("stream.execution.environment.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<String> STREAM_EXECUTION_JOB_NAME = ConfigOptions.key("stream.execution.job.name")
            .stringType()
            .defaultValue("ETL-METRIC");

    public static final ConfigOption<Integer> SESSION_RECORD_COMPLETED_PARALLELISM = ConfigOptions.key("session.record.completed.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<String> SESSION_RECORD_COMPLETED_TOPIC = ConfigOptions.key("session.record.completed.topic")
            .stringType()
            .defaultValue("SESSION-RECORD-TEMP");

    public static final ConfigOption<String> SESSION_RECORD_COMPLETED_GROUP_ID = ConfigOptions.key("session.record.completed.group.id")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> KAFKA_INPUT_BOOTSTRAP_SERVERS = ConfigOptions.key("kafka.input.bootstrap.servers")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> CN_RECORD_TOPIC = ConfigOptions.key("cn.record.topic")
            .stringType()
            .defaultValue("SESSION-RECORD-CN");

    public static final ConfigOption<Integer> CN_RECORD_PARALLELISM = ConfigOptions.key("cn.record.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<String> KAFKA_OUTPUT_BOOTSTRAP_SERVERS = ConfigOptions.key("kafka.output.bootstrap.servers")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ZOOKEEPER_SERVERS = ConfigOptions.key("zookeeper.servers")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> INPUT_SASL_JAAS_CONFIG_FLAG = ConfigOptions.key("input.sasl.jaas.config.flag")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<String> SASL_JAAS_CONFIG_USER = ConfigOptions.key("sasl.jaas.config.user")
            .stringType()
            .defaultValue("admin");

    public static final ConfigOption<String> SASL_JAAS_CONFIG_PASSWORD = ConfigOptions.key("sasl.jaas.config.password")
            .stringType()
            .defaultValue("galaxy2019");

    public static final ConfigOption<Integer> OUTPUT_SASL_JAAS_CONFIG_FLAG = ConfigOptions.key("output.sasl.jaas.config.flag")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<String> FLINK_ENABLE_CHECKPOINT_FLAG = ConfigOptions.key("flink.enable.checkpoint.flag")
            .stringType()
            .defaultValue("1");

    public static final ConfigOption<Integer> FLINK_ENABLE_CHECKPOINT_TIME = ConfigOptions.key("flink.enable.checkpoint.time")
            .intType()
            .defaultValue(60);

    public static final ConfigOption<Integer> FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS = ConfigOptions.key("flink.min.pause.between.checkpoints")
            .intType()
            .defaultValue(30);

    public static final ConfigOption<Integer> FLINK_CHECKPOINT_TIMEOUT = ConfigOptions.key("flink.checkpoint.timeout")
            .intType()
            .defaultValue(30);

    public static final ConfigOption<Integer> FLINK_TOLERABLE_CHECKPOINT_FAILURE_NUMBER = ConfigOptions.key("flink.tolerable.checkpoint.failure.number")
            .intType()
            .defaultValue(2);

    public static final ConfigOption<Integer> FLINK_MAX_CONCURRENT_CHECKPOINT = ConfigOptions.key("flink.max.concurrent.checkpoint")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<String> KAFKA_PRODUCER_RETRIES = ConfigOptions.key("kafka.producer.retries")
            .stringType()
            .defaultValue("0");

    public static final ConfigOption<String> KAFKA_PRODUCER_LINGER_MS = ConfigOptions.key("kafka.producer.linger.ms")
            .stringType()
            .defaultValue("10");

    public static final ConfigOption<String> KAFKA_PRODUCER_REQUEST_TIMEOUT_MS = ConfigOptions.key("kafka.producer.request.timeout.ms")
            .stringType()
            .defaultValue("30000");

    public static final ConfigOption<String> KAFKA_PRODUCER_BATCH_SIZE = ConfigOptions.key("kafka.producer.batch.size")
            .stringType()
            .defaultValue("262144");

    public static final ConfigOption<String> KAFKA_PRODUCER_BUFFER_MEMORY = ConfigOptions.key("kafka.producer.buffer.memory")
            .stringType()
            .defaultValue("134217728");

    public static final ConfigOption<String> KAFKA_PRODUCER_MAX_REQUEST_SIZE = ConfigOptions.key("kafka.producer.max.request.size")
            .stringType()
            .defaultValue("10485760");

    public static final ConfigOption<String> KAFKA_PRODUCER_COMPRESSION_TYPE = ConfigOptions.key("kafka.producer.compression.type")
            .stringType()
            .defaultValue("snappy");

    public static final ConfigOption<Integer> HTTP_POOL_MAX_CONNECTION = ConfigOptions.key("http.pool.max.connection")
            .intType()
            .defaultValue(400);

    public static final ConfigOption<Integer> HTTP_POOL_MAX_PER_ROUTE = ConfigOptions.key("http.pool.max.per.route")
            .intType()
            .defaultValue(80);

    public static final ConfigOption<Integer> HTTP_POOL_REQUEST_TIMEOUT = ConfigOptions.key("http.pool.request.timeout")
            .intType()
            .defaultValue(60000);

    public static final ConfigOption<Integer> HTTP_POOL_CONNECT_TIMEOUT = ConfigOptions.key("http.pool.connect.timeout")
            .intType()
            .defaultValue(60000);

    public static final ConfigOption<Integer> HTTP_POOL_RESPONSE_TIMEOUT = ConfigOptions.key("http.pool.response.timeout")
            .intType()
            .defaultValue(60000);

    public static final ConfigOption<String> NACOS_SERVER_ADDR = ConfigOptions.key("nacos.server.addr")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> NACOS_USERNAME = ConfigOptions.key("nacos.username")
            .stringType()
            .defaultValue("nacos");

    public static final ConfigOption<String> NACOS_PASSWORD = ConfigOptions.key("nacos.password")
            .stringType()
            .defaultValue("nacos");

    public static final ConfigOption<String> NACOS_NAMESPACE = ConfigOptions.key("nacos.namespace")
            .stringType()
            .defaultValue("");

    public static final ConfigOption<String> NACOS_DATA_ID = ConfigOptions.key("nacos.data.id")
            .stringType()
            .defaultValue("knowledge_base.json");

    public static final ConfigOption<String> NACOS_GROUP = ConfigOptions.key("nacos.group")
            .stringType()
            .defaultValue("DEFAULT_GROUP");

    public static final ConfigOption<Integer> NACOS_READ_TIMEOUT = ConfigOptions.key("nacos.read.timeout")
            .intType()
            .defaultValue(5000);

    public static final ConfigOption<String> CLUSTER_OR_SINGLE = ConfigOptions.key("cluster.or.single")
            .stringType()
            .defaultValue("single");

    public static final ConfigOption<String> HDFS_URI_NS1 = ConfigOptions.key("hdfs.uri.nn1")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> HDFS_URI_NS2 = ConfigOptions.key("hdfs.uri.nn2")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> HDFS_PATH = ConfigOptions.key("hdfs.path")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> HDFS_USER = ConfigOptions.key("hdfs.user")
            .stringType()
            .defaultValue("cn");

    public static final ConfigOption<String> DOWNLOAD_PATH = ConfigOptions.key("download.path")
            .stringType()
            .defaultValue("/opt/data/");

    public static final ConfigOption<Integer> KNOWLEDGE_FILE_CHECK_NUMBER = ConfigOptions.key("knowledge.file.check.number")
            .intType()
            .defaultValue(3);

    public static final ConfigOption<Integer> WATERMARK_SECONDS = ConfigOptions.key("watermark.seconds")
            .intType()
            .defaultValue(1);
}
