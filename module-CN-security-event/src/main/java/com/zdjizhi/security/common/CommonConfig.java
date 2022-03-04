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
            propService.load(CommonConfigurations.class.getClassLoader().getResourceAsStream("SecurityEvent.properties"));
            configurations = new CommonConfigurations(propService);
        } catch (Exception e) {
            logger.error("加载security.properties配置文件失败");
            System.exit(1);
        }
    }

    public static final int FLINK_WATERMARK_MAX_ORDERNESS = configurations.getIntProperty("flink.watermark.max.time");
    public static final int FLINK_WINDOW_TIME = configurations.getIntProperty("flink.window.time");
    public static final int ES_MAX_FlUSH = configurations.getIntProperty("es.max.flush");
}
