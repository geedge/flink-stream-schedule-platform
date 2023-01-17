package com.zdjizhi.security.common;

import com.zdjizhi.base.utils.CommonConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * @ Author pengfeixu
 * @ Date 2022/2/25
 * @ Description
 */
public class CommonConfig {
    private static CommonConfigurations configurations;
    private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);

    static {
        Properties propService;
        try {
            propService = new Properties();
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("security-event-detection.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载security.properties配置文件失败");
            System.exit(1);
        }
    }

    public static final int FLINK_WATERMARK_MAX_ORDERNESS = configurations.getIntProperty("flink.watermark.max.time");
    public static final int END_TIME = configurations.getIntProperty("end.time");
    public static final int FLINK_WINDOW_TIME = configurations.getIntProperty("flink.window.time");
    public static final int ES_MAX_FLUSH = configurations.getIntProperty("es.max.flush");

//    public static final int SECURITY_SOURCE_PARALLELISM=configurations.getIntProperty("security.source.parallelism");
    public static final int SECURITY_SERVER_PARALLELISM=configurations.getIntProperty("security.server.parallelism");
    public static final int SECURITY_DOMAIN_PARALLELISM=configurations.getIntProperty("security.domain.parallelism");
    public static final int SECURITY_URL_PARALLELISM=configurations.getIntProperty("security.url.parallelism");

    public static final String SECURITY_SERVER_SLOT_NAME=configurations.getStringProperty("security.server.slot.name");
    public static final String SECURITY_DOMAIN_SLOT_NAME=configurations.getStringProperty("security.domain.slot.name");
    public static final String SECURITY_URL_SLOT_NAME=configurations.getStringProperty("security.url.slot.name");

    public static final String ES_SECURITY_TABLE_NAME = configurations.getStringProperty("es.security.table.name");
}
