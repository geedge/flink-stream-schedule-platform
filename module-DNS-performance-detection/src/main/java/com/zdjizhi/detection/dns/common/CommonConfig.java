package com.zdjizhi.detection.dns.common;

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
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("DNS-performance-detection.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载DNS-performance-detection.properties配置文件失败");
            System.exit(1);
        }
    }

    public static final int FLINK_WATERMARK_MAX_ORDERNESS = configurations.getIntProperty("flink.watermark.max.orderness");
    public static final int FLINK_WINDOW_MAX_TIME = configurations.getIntProperty("flink.window.max.time");

    public static final double DNS_ERROR_DETECTION_INFO_THRESHOLD = configurations.getDoubleProperty("dns.error.detection.info.threshold");
    public static final double DNS_ERROR_DETECTION_LOW_THRESHOLD = configurations.getDoubleProperty("dns.error.detection.low.threshold");
    public static final double DNS_ERROR_DETECTION_MEDIUM_THRESHOLD = configurations.getDoubleProperty("dns.error.detection.medium.threshold");
    public static final double DNS_ERROR_DETECTION_HIGH_THRESHOLD = configurations.getDoubleProperty("dns.error.detection.high.threshold");
    public static final double DNS_ERROR_DETECTION_CRITICAL_THRESHOLD = configurations.getDoubleProperty("dns.error.detection.critical.threshold");

    public static final double DNS_RESPONSE_DETECTION_INFO_THRESHOLD = configurations.getDoubleProperty("dns.response.detection.info.threshold");
    public static final double DNS_RESPONSE_DETECTION_LOW_THRESHOLD = configurations.getDoubleProperty("dns.response.detection.low.threshold");
    public static final double DNS_RESPONSE_DETECTION_MEDIUM_THRESHOLD = configurations.getDoubleProperty("dns.response.detection.medium.threshold");
    public static final double DNS_RESPONSE_DETECTION_HIGH_THRESHOLD = configurations.getDoubleProperty("dns.response.detection.high.threshold");
    public static final double DNS_RESPONSE_DETECTION_CRITICAL_THRESHOLD = configurations.getDoubleProperty("dns.response.detection.critical.threshold");


    public static final int BULK_FLUSH_MAX_ACTION = configurations.getIntProperty("bulk.flush.max.action");


}
