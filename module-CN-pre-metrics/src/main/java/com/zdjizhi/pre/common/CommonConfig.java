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
            e.printStackTrace(System.out);
            logger.error("加载top-metrics.properties配置文件失败", e);
            System.exit(1);
        }
    }


    public static final int PRE_METRICS_WINDOW_TIME = CommonConfigurations.getIntProperty("pre.metrics.window.time");
    public static final int PRE_METRICS_ROUND_SCALE = CommonConfigurations.getIntProperty("pre.metrics.round.scale");
    public static final String METRIC_IP_TOPIC = CommonConfigurations.getStringProperty("metric.ip.topic");
    public static final String METRIC_REGION_TOPIC = CommonConfigurations.getStringProperty("metric.region.topic");
    public static final String METRIC_ASN_TOPIC = CommonConfigurations.getStringProperty("metric.asn.topic");
    public static final String METRIC_IDC_RENTER_TOPIC = CommonConfigurations.getStringProperty("metric.idc.renter.topic");
    public static final String METRIC_APPLICATION_TOPIC = CommonConfigurations.getStringProperty("metric.application.topic");
    public static final String METRIC_DOMAIN_TOPIC = CommonConfigurations.getStringProperty("metric.domain.topic");
    public static final String METRIC_HTTP_HOST_TOPIC = CommonConfigurations.getStringProperty("metric.http.host.topic");
    public static final String METRIC_SSL_SNI_TOPIC = CommonConfigurations.getStringProperty("metric.ssl.sni.topic");
    public static final String METRIC_PROTOCOL_TOPIC = CommonConfigurations.getStringProperty("metric.protocol.topic");
    public static final String METRIC_LINK_TOPIC = CommonConfigurations.getStringProperty("metric.link.topic");
    public static final String METRIC_RELATION_TOPIC = CommonConfigurations.getStringProperty("metric.relation.topic");
    public static final String METRIC_ATTRIBUTE_TOPIC = CommonConfigurations.getStringProperty("metric.attribute.topic");

    public static final int METRIC_OUTPUT_PARALLELISM = CommonConfigurations.getIntProperty("metric.output.parallelism");

}
