package com.zdjizhi.dos.common;
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
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("dos-detection.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载dos-detection.properties配置文件失败");
            System.exit(1);
        }
    }

    public static final int KAFKA_OUTPUT_METRIC_PARALLELISM = configurations.getIntProperty("kafka.output.metric.parallelism");
    public static final String KAFKA_OUTPUT_METRIC_TOPIC_NAME = configurations.getStringProperty("kafka.output.metric.topic.name");
    public static final int KAFKA_OUTPUT_EVENT_PARALLELISM = configurations.getIntProperty("kafka.output.event.parallelism");
    public static final String KAFKA_OUTPUT_EVENT_TOPIC_NAME = configurations.getStringProperty("kafka.output.event.topic.name");

    public static final String HBASE_ZOOKEEPER_QUORUM = configurations.getStringProperty("hbase.zookeeper.quorum");
    public static final int HBASE_CLIENT_OPERATION_TIMEOUT = configurations.getIntProperty("hbase.client.operation.timeout");
    public static final int HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = configurations.getIntProperty("hbase.client.scanner.timeout.period");

    public static final String HBASE_BASELINE_TABLE_NAME = configurations.getStringProperty("hbase.baseline.table.name");
    public static final int HBASE_BASELINE_TOTAL_NUM = configurations.getIntProperty("hbase.baseline.total.num");
    public static final int HBASE_BASELINE_TTL = configurations.getIntProperty("hbase.baseline.ttl");

    public static final int FLINK_FIRST_AGG_PARALLELISM = configurations.getIntProperty("flink.first.agg.parallelism");
    public static final int FLINK_DETECTION_MAP_PARALLELISM = configurations.getIntProperty("flink.detection.map.parallelism");
    public static final int FLINK_WATERMARK_MAX_ORDERNESS = configurations.getIntProperty("flink.watermark.max.orderness");
    public static final int FLINK_WINDOW_MAX_TIME = configurations.getIntProperty("flink.window.max.time");

    public static final int SOURCE_IP_LIST_LIMIT = configurations.getIntProperty("source.ip.list.limit");
    public static final int DESTINATION_IP_PARTITION_NUM = configurations.getIntProperty("destination.ip.partition.num");
    public static final int DATA_CENTER_ID_NUM = configurations.getIntProperty("data.center.id.num");

    public static final String IP_MMDB_PATH = configurations.getStringProperty("ip.mmdb.path");

    public static final int STATIC_SENSITIVITY_THRESHOLD = configurations.getIntProperty("static.sensitivity.threshold");
    public static final double BASELINE_SENSITIVITY_THRESHOLD = configurations.getDoubleProperty("baseline.sensitivity.threshold");

    public static final double BASELINE_SESSIONS_MINOR_THRESHOLD = configurations.getDoubleProperty("baseline.sessions.minor.threshold");
    public static final double BASELINE_SESSIONS_WARNING_THRESHOLD = configurations.getDoubleProperty("baseline.sessions.warning.threshold");
    public static final double BASELINE_SESSIONS_MAJOR_THRESHOLD = configurations.getDoubleProperty("baseline.sessions.major.threshold");
    public static final double BASELINE_SESSIONS_SEVERE_THRESHOLD = configurations.getDoubleProperty("baseline.sessions.severe.threshold");
    public static final double BASELINE_SESSIONS_CRITICAL_THRESHOLD = configurations.getDoubleProperty("baseline.sessions.critical.threshold");

    public static final String BIFANG_SERVER_URI = configurations.getStringProperty("bifang.server.uri");
    public static final String BIFANG_SERVER_TOKEN = configurations.getStringProperty("bifang.server.token");
    public static final String BIFANG_SERVER_ENCRYPTPWD_PATH = configurations.getStringProperty("bifang.server.encryptpwd.path");
    public static final String BIFANG_SERVER_LOGIN_PATH = configurations.getStringProperty("bifang.server.login.path");
    public static final String BIFANG_SERVER_POLICY_THRESHOLD_PATH = configurations.getStringProperty("bifang.server.policy.threshold.path");

    public static final int HTTP_POOL_MAX_CONNECTION = configurations.getIntProperty("http.pool.max.connection");
    public static final int HTTP_POOL_MAX_PER_ROUTE = configurations.getIntProperty("http.pool.max.per.route");
    public static final int HTTP_POOL_REQUEST_TIMEOUT = configurations.getIntProperty("http.pool.request.timeout");
    public static final int HTTP_POOL_CONNECT_TIMEOUT = configurations.getIntProperty("http.pool.connect.timeout");
    public static final int HTTP_POOL_RESPONSE_TIMEOUT = configurations.getIntProperty("http.pool.response.timeout");

    public static final int STATIC_THRESHOLD_SCHEDULE_MINUTES = configurations.getIntProperty("static.threshold.schedule.minutes");
    public static final int BASELINE_THRESHOLD_SCHEDULE_DAYS = configurations.getIntProperty("baseline.threshold.schedule.days");

}
