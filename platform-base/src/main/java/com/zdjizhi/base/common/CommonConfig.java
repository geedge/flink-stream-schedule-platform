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

    public static final int DOS_SKETCH_RECORD_PARALLELISM = configurations.getIntProperty("dos.sketch.record.parallelism");
    public static final String DOS_SKETCH_RECORD_TOPIC = configurations.getStringProperty("dos.sketch.record.topic");
    public static final String DOS_SKETCH_RECORD_GROUP_ID = configurations.getStringProperty("dos.sketch.record.group.id");

    public static final int SESSION_RECORD_COMPLETED_PARALLELISM = configurations.getIntProperty("session.record.completed.parallelism");
    public static final String SESSION_RECORD_COMPLETED_TOPIC = configurations.getStringProperty("session.record.completed.topic");
    public static final String SESSION_RECORD_COMPLETED_GROUP_ID = configurations.getStringProperty("session.record.completed.group.id");

    public static final String KAFKA_INPUT_BOOTSTRAP_SERVERS = configurations.getStringProperty("kafka.input.bootstrap.servers");

    public static final String TSG_BASE_TOP_TOPIC = configurations.getStringProperty("tsg.base.top.topic");
    public static final int TSG_BASE_TOP_PARALLELISM = configurations.getIntProperty("tsg.base.top.parallelism");

    public static final String CN_RECORD_TOPIC = configurations.getStringProperty("cn.record.topic");
    public static final int CN_RECORD_PARALLELISM = configurations.getIntProperty("cn.record.parallelism");

    public static final String TSG_BASE_COUNTRY_CITY_TOPIC = configurations.getStringProperty("tsg.base.country.city.topic");
    public static final int TSG_BASE_COUNTRY_CITY_PARALLELISM = configurations.getIntProperty("tsg.base.country.city.parallelism");

    public static final String KAFKA_OUTPUT_BOOTSTRAP_SERVERS = configurations.getStringProperty("kafka.output.bootstrap.servers");
    public static final String ZOOKEEPER_SERVERS = configurations.getStringProperty("zookeeper.servers");

    public static final int DATA_CENTER_ID_NUM = configurations.getIntProperty("data.center.id.num");

    public static final String ES_HOST = configurations.getStringProperty("es.host");
    public static final int ES_SINK_PARALLELISM=configurations.getIntProperty("es.sink.parallelism");

    public static final String SASL_JAAS_CONFIG_USER = configurations.getStringProperty("sasl.jaas.config.user");
    public static final String SASL_JAAS_CONFIG_PASSWORD = configurations.getStringProperty("sasl.jaas.config.password");

    public static final int INPUT_SASL_JAAS_CONFIG_FLAG = configurations.getIntProperty("input.sasl.jaas.config.flag");
    public static final int OUTPUT_SASL_JAAS_CONFIG_FLAG = configurations.getIntProperty("output.sasl.jaas.config.flag");

    public static final String IP_MMDB_PATH = configurations.getStringProperty("ip.mmdb.path");
    public static final String WEBSKT_PATH = configurations.getStringProperty("webskt.path");
    public static final String DNS_PATH = configurations.getStringProperty("dns.path");
    public static final String APPSKT_PATH = configurations.getStringProperty("appskt.path");
    public static final String WHOIS_PATH = configurations.getStringProperty("whois.path");
    public static final String ICP_PATH = configurations.getStringProperty("icp.path");
    public static final String IOC_PATH = configurations.getStringProperty("ioc.path");
    public static final String LINK_PATH = configurations.getStringProperty("link.path");
    public static final String ISP_PATH = configurations.getStringProperty("isp.path");
    public static final String IDC_RENTER_PATH = configurations.getStringProperty("idc.renter.path");

    public static final String FLINK_ENABLE_CHECKPOINT_FLAG = configurations.getStringProperty("flink.enable.checkpoint.flag");
    public static final int FLINK_ENABLE_CHECKPOINT_TIME = configurations.getIntProperty("flink.enable.checkpoint.time");
    public static final int FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS = configurations.getIntProperty("flink.min.pause.between.checkpoints");
    public static final int FLINK_CHECKPOINT_TIMEOUT = configurations.getIntProperty("flink.checkpoint.timeout");
    public static final int FLINK_TOLERABLE_CHECKPOINT_FAILURE_NUMBER = configurations.getIntProperty("flink.tolerable.checkpoint.failure.number");
    public static final int FLINK_MAX_CONCURRENT_CHECKPOINT = configurations.getIntProperty("flink.max.concurrent.checkpoint");





}
