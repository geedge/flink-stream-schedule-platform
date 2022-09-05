package com.zdjizhi.pre.common;

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
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("pre-metrics.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载top-metrics.properties配置文件失败");
            System.exit(1);
        }
    }


    public static final int PRE_METRICS_WINDOW_TIME = configurations.getIntProperty("pre.metrics.window.time");
    public static final int PRE_METRICS_ROUND_SCALE = configurations.getIntProperty("pre.metrics.round.scale");
    public static final String METRIC_IP_TOPIC = configurations.getStringProperty("metric.ip.topic");
    public static final String METRIC_REGION_TOPIC = configurations.getStringProperty("metric.region.topic");
    public static final String METRIC_ASN_TOPIC = configurations.getStringProperty("metric.asn.topic");
    public static final String METRIC_IDC_RENTER_TOPIC = configurations.getStringProperty("metric.idc.renter.topic");
    public static final String METRIC_APPLICATION_TOPIC = configurations.getStringProperty("metric.application.topic");
    public static final String METRIC_DOMAIN_TOPIC = configurations.getStringProperty("metric.domain.topic");
    public static final String METRIC_HTTP_HOST_TOPIC = configurations.getStringProperty("metric.http.host.topic");
    public static final String METRIC_SSL_SNI_TOPIC = configurations.getStringProperty("metric.ssl.sni.topic");
    public static final String METRIC_PROTOCOL_TOPIC = configurations.getStringProperty("metric.protocol.topic");

    public static final int METRIC_OUTPUT_PARALLELISM = configurations.getIntProperty("metric.output.parallelism");

}
