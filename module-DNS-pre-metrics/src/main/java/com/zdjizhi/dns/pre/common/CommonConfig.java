package com.zdjizhi.dns.pre.common;

import com.zdjizhi.base.utils.CommonConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CommonConfig {
    private static CommonConfigurations configurations;
    private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);

    static {
        Properties propService;
        try {
            propService = new Properties();
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("dns_pre_metrics.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载dns_pre_metrics.properties配置文件失败");
            System.exit(1);
        }
    }


    public static final int DNS_PRE_METRICS_WINDOW_TIME = configurations.getIntProperty("dns.pre.metrics.window.time");
    public static final int DNS_PRE_METRICS_ROUND_SCALE = configurations.getIntProperty("dns.pre.metrics.round.scale");
    public static final String METRIC_DNS_SERVER_IP_TOPIC = configurations.getStringProperty("dns.metric.server.ip.topic");
    public static final String METRIC_DNS_QNAME_TOPIC = configurations.getStringProperty("dns.metric.qname.topic");
    public static final String METRIC_DNS_QTYPE_TOPIC = configurations.getStringProperty("dns.metric.qtype.topic");
    public static final String METRIC_DNS_RCODE_TOPIC = configurations.getStringProperty("dns.metric.rcode.topic");
    public static final String METRIC_DNS_RRA_TOPIC = configurations.getStringProperty("dns.metric.rra.topic");
    public static final String METRIC_DNS_RRAAAA_TOPIC = configurations.getStringProperty("dns.metric.rraaaa.topic");
    public static final String METRIC_DNS_RRCNAME_TOPIC = configurations.getStringProperty("dns.metric.rrcname.topic");

    public static final int DNS_METRIC_OUTPUT_PARALLELISM = configurations.getIntProperty("dns.metric.output.parallelism");
}
